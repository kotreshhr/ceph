// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPHFS_MIRROR_UTILS_H
#define CEPHFS_MIRROR_UTILS_H

#include "Types.h"

namespace cephfs {
namespace mirror {

int connect(std::string_view client_name, std::string_view cluster_name,
            RadosRef *cluster, std::string_view mon_host={}, std::string_view cephx_key={},
            std::vector<const char *> args={});

int mount(RadosRef cluster, const Filesystem &filesystem, bool cross_check_fscid,
          MountRef *mount);

std::string get_mon_host(CephContext* cct);
int get_loaded_cephx_key(std::string *secret);

// Kernel CephFS mount helper.
// Mounts the filesystem via the kernel client and returns an open fd
// to the mount root. The mountpoint is immediately unmounted and removed
// so that only the holder of the fd can access the mount (via /proc/self/fd/
// or openat()). This prevents other processes from using the mountpoint.
class KernelMount {
public:
  KernelMount() = default;
  ~KernelMount();

  KernelMount(const KernelMount &) = delete;
  KernelMount &operator=(const KernelMount &) = delete;

  // Setup the kernel mount. Extracts mon_addrs and fsid from the
  // libcephfs RadosRef, performs `mount -t ceph`, opens an fd on the
  // mountpoint, then unmounts and removes the temp directory.
  // The fd remains valid and usable via openat().
  int init(RadosRef rados_cluster, const std::string mon_host,
           const std::string &fs_name, const std::string &client_name,
           const std::string &key);

  // Shutdown: close the root fd.
  void shutdown();

  // Return the root directory fd. Use with openat() for relative path ops.
  int root_fd() const { return m_root_fd; }

  bool is_valid() const { return m_root_fd >= 0; }

private:
  int m_root_fd = -1;
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_UTILS_H
