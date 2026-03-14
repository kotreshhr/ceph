// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/errno.h"
#include "auth/KeyRing.h"
#include <sys/mount.h>

#include "Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::Utils " << __func__

namespace {

int sys_mount(const char *src, const char *tgt, const char *fs,
              unsigned long flags, const void *data) {
  return ::mount(src, tgt, fs, flags, data);
}
int sys_umount2(const char *target, int flags) {
  return ::umount2(target, flags);
}

} // anonymous namespace

namespace cephfs {
namespace mirror {

int connect(std::string_view client_name, std::string_view cluster_name,
            RadosRef *cluster, std::string_view mon_host, std::string_view cephx_key,
            std::vector<const char *> args) {
  dout(20) << ": connecting to cluster=" << cluster_name << ", client=" << client_name
           << ", mon_host=" << mon_host << dendl;

  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (client_name.empty() || !iparams.name.from_str(client_name)) {
    derr << ": error initializing cluster handle for " << cluster_name << dendl;
    return -EINVAL;
  }

  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY,
                                    CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
  if (mon_host.empty()) {
    cct->_conf->cluster = cluster_name;
  }

  int r = cct->_conf.parse_config_files(nullptr, nullptr, 0);
  if (r < 0 && r != -ENOENT) {
    derr << ": could not read ceph conf: " << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  cct->_conf.parse_env(cct->get_module_type());

  if (!args.empty()) {
    r = cct->_conf.parse_argv(args);
    if (r < 0) {
      derr << ": could not parse command line args: " << cpp_strerror(r) << dendl;
      cct->put();
      return r;
    }
  }
  cct->_conf.parse_env(cct->get_module_type());

  if (!mon_host.empty()) {
    r = cct->_conf.set_val("mon_host", std::string(mon_host));
    if (r < 0) {
      derr << "failed to set mon_host config: " << cpp_strerror(r) << dendl;
      cct->put();
      return r;
    }
  }
  if (!cephx_key.empty()) {
    r = cct->_conf.set_val("key", std::string(cephx_key));
    if (r < 0) {
      derr << "failed to set key config: " << cpp_strerror(r) << dendl;
      cct->put();
      return r;
    }
  }

  dout(10) << ": using mon addr=" << cct->_conf.get_val<std::string>("mon_host") << dendl;

  cluster->reset(new librados::Rados());

  r = (*cluster)->init_with_context(cct);
  ceph_assert(r == 0);
  cct->put();

  r = (*cluster)->connect();
  if (r < 0) {
    derr << ": error connecting to " << cluster_name << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  dout(10) << ": connected to cluster=" << cluster_name << " using client="
           << client_name << dendl;

  return 0;
}

int mount(RadosRef cluster, const Filesystem &filesystem, bool cross_check_fscid,
          MountRef *mount) {
  dout(20) << ": filesystem=" << filesystem << dendl;

  ceph_mount_info *cmi;
  int r = ceph_create_with_context(&cmi, reinterpret_cast<CephContext*>(cluster->cct()));
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = ceph_conf_set(cmi, "client_mount_uid", "0");
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = ceph_conf_set(cmi, "client_mount_gid", "0");
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  // mount timeout applies for local and remote mounts.
  auto mount_timeout = g_ceph_context->_conf.get_val<std::chrono::seconds>
    ("cephfs_mirror_mount_timeout").count();
  r = ceph_set_mount_timeout(cmi, mount_timeout);
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = ceph_init(cmi);
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = ceph_select_filesystem(cmi, filesystem.fs_name.c_str());
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = ceph_mount(cmi, NULL);
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  auto fs_id = ceph_get_fs_cid(cmi);
  if (cross_check_fscid && fs_id != filesystem.fscid) {
    // this can happen in the most remotest possibility when a
    // filesystem is deleted and recreated with the same name.
    // since all this is driven asynchronously, we were able to
    // mount the recreated filesystem. so bubble up the error.
    // cleanup will eventually happen since a mirror disable event
    // would have been queued.
    derr << ": filesystem-id mismatch " << fs_id << " vs " << filesystem.fscid
         << dendl;
    // ignore errors, we are shutting down anyway.
    ceph_unmount(cmi);
    return -EINVAL;
  }

  dout(10) << ": mounted filesystem=" << filesystem << dendl;

  *mount = cmi;
  return 0;
}

std::string get_mon_host(CephContext* cct) {
  std::string mon_host;
  if (auto mon_addrs = cct->get_mon_addrs();
      mon_addrs != nullptr && !mon_addrs->empty()) {
    CachedStackStringStream css;
    for (auto it = mon_addrs->begin(); it != mon_addrs->end(); ++it) {
      if (it != mon_addrs->begin()) {
        *css << ",";
      }
      *css << *it;
    }
    mon_host = css->str();
  } else {
    ldout(cct, 20) << ": falling back to mon_host in conf" << dendl;
    mon_host = cct->_conf.get_val<std::string>("mon_host");
  }
  ldout(cct, 20) << ": mon_host=" << mon_host << dendl;
  return mon_host;
}

int get_loaded_cephx_key(std::string *secret)
{
  auto conf = g_ceph_context->_conf;

  EntityName entity;
  entity.from_str(conf->name.to_str());

  KeyRing keyring;
  int r = keyring.load(g_ceph_context, conf->keyring);
  if (r < 0)
    return r;

  CryptoKey key;
  if (!keyring.get_secret(entity, key))
    return -ENOENT;

  std::string secret_str;
  key.encode_base64(secret_str);
  *secret = secret_str;
  return 0;
}

static std::string normalize_mon_host(const std::string& in)
{
  std::string s = in;

  // remove brackets if present
  if (!s.empty() && s.front() == '[')
    s.erase(0, 1);
  if (!s.empty() && s.back() == ']')
    s.pop_back();

  std::vector<std::string> result;
  std::stringstream ss(s);
  std::string item;

  while (std::getline(ss, item, ',')) {
    // trim whitespace
    item.erase(0, item.find_first_not_of(" \t"));
    item.erase(item.find_last_not_of(" \t") + 1);

    // remove v1:/v2: prefix
    if (item.rfind("v1:", 0) == 0 || item.rfind("v2:", 0) == 0)
      item = item.substr(3);

    // strip trailing /... (like /0)
    auto slash = item.find('/');
    if (slash != std::string::npos)
      item = item.substr(0, slash);

    if (!item.empty())
      result.push_back(item);
  }

  // join with comma
  std::string out;
  for (size_t i = 0; i < result.size(); ++i) {
    if (i)
      out += ",";
    out += result[i];
  }

  return out;
}

static std::string strip_client_prefix(const std::string& in)
{
  const std::string prefix = "client.";
  if (in.rfind(prefix, 0) == 0) {  // starts with "client."
    return in.substr(prefix.size());
  }
  return in;
}

// --- KernelMount implementation ---

KernelMount::~KernelMount() {
  shutdown();
}

int KernelMount::init(RadosRef rados_cluster, const std::string mon_host,
                      const std::string &fs_name, const std::string &client_name,
                      const std::string &key) {
  int r = 0;
  if (m_root_fd >= 0) {
    derr << ": kernel mount already initialized" << dendl;
    return -EEXIST;
  }

  std::string fsid;
  rados_cluster->cluster_fsid(&fsid);
  std::string mon_addrs = normalize_mon_host(mon_host);
  std::string client_id = strip_client_prefix(client_name);

  // Create a secure temporary mountpoint
  char tmpdir[] = "/tmp/cephfs_mirror_XXXXXX";
  if (::mkdtemp(tmpdir) == nullptr) {
    r = -errno;
    derr << ": failed to create temp mountpoint: "
         << cpp_strerror(r) << dendl;
    return r;
  }

  // Try new mount syntax first (requires fsid)
  // Device:  name@fsid.fs_name=/
  // Options: secret=<key>,mon_addr=IP:PORT/IP:PORT,ms_mode=prefer-crc
  r = -EINVAL;
  if (!fsid.empty()) {
    std::string mount_dev = client_id + "@" + fsid + "." + fs_name + "=/";

    std::string mon_addr_opt = mon_addrs;
    std::replace(mon_addr_opt.begin(), mon_addr_opt.end(), ',', '/');

    std::string mount_opts = "name=" + client_id
                           + ",secret=" + key
                           + ",mon_addr=" + mon_addr_opt
                           + ",ms_mode=prefer-crc";

    dout(10) << ": trying new mount syntax: dev=" << client_id
             << "@<fsid>." << fs_name << "=/"
             << " opts=name=" << client_id
             << ",secret=<hidden>,mon_addr=" << mon_addr_opt
             << ",ms_mode=prefer-crc" << dendl;
    r = sys_mount(mount_dev.c_str(), tmpdir, "ceph", 0, mount_opts.c_str());
    if (r < 0) {
      r = -errno;
      dout(5) << ": new mount syntax failed: " << cpp_strerror(r)
              << ", will try old syntax" << dendl;
    }
  }

  // Fall back to old mount syntax if new syntax failed with EINVAL
  // Device:  IP:PORT,IP:PORT:/
  // Options: name=<client>,secret=<key>,mds_namespace=<fsname>
  if (r < 0) {
    std::string mount_dev = mon_addrs + ":/";
    std::string mount_opts = "name=" + client_id
                           + ",secret=" + key
                           + ",mds_namespace=" + fs_name;
    if (!fsid.empty()) {
      mount_opts += ",fsid=" + fsid;
    }

    dout(10) << ": trying old mount syntax: dev=" << mount_dev
             << " mount_point=" << tmpdir
             << " mount_opts=" << mount_opts
             << dendl;

    r = sys_mount(mount_dev.c_str(), tmpdir, "ceph", 0, mount_opts.c_str());
    if (r < 0) {
      r = -errno;
      derr << ": kernel mount failed (both syntaxes) at " << tmpdir
           << ": " << cpp_strerror(r) << dendl;
      ::rmdir(tmpdir);
      return r;
    }
  }

  dout(10) << ": kernel mounted at " << tmpdir << dendl;

  // Open an fd on the mountpoint root — keeps the mount alive
  int fd = ::open(tmpdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) {
    r = -errno;
    derr << ": failed to open kernel mount root: "
         << cpp_strerror(r) << dendl;
    sys_umount2(tmpdir, MNT_DETACH);
    ::rmdir(tmpdir);
    return r;
  }

  // Lazy unmount so no other process can access via the path
  r = sys_umount2(tmpdir, MNT_DETACH);
  if (r < 0) {
    r = -errno;
    derr << ": failed to lazy-unmount " << tmpdir << ": "
         << cpp_strerror(r) << dendl;
    // Non-fatal — mount still works, just also visible at tmpdir
  }

  // Remove the temporary directory
  ::rmdir(tmpdir);

  m_root_fd = fd;
  dout(10) << ": kernel mount secured, root_fd=" << m_root_fd << dendl;
  return 0;
}

void KernelMount::shutdown() {
  if (m_root_fd >= 0) {
    dout(10) << ": closing kernel mount root_fd=" << m_root_fd << dendl;
    ::close(m_root_fd);
    m_root_fd = -1;
  }
}

} // namespace mirror
} // namespace cephfs
