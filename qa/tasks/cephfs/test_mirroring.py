import json
import logging
import random
import time
import functools

from io import StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while

log = logging.getLogger(__name__)


# Exceptions to retry in test assertions
RETRY_EXCEPTIONS = (AssertionError, KeyError, IndexError, CommandFailedError)


def retry_assert(timeout=60, interval=1):
    """
    Retry a test helper until assertions inside it pass or timeout expires.
    Prints retry count on each failure.
    """
    tries = int(timeout/interval)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            attempt = 1

            with safe_while(sleep=interval, tries=tries, action=f"retry {func.__name__}") as proceed:
                while proceed():
                    try:
                        return func(*args, **kwargs)
                    except RETRY_EXCEPTIONS as e:
                        last_exc = e
                        log.debug(
                            f"[retry_assert] {func.__name__}: "
                            f"attempt {attempt} failed ({type(e).__name__}), retrying..."
                        )
                        attempt += 1
            # Final failure
            if last_exc is not None and hasattr(last_exc, "res"):
                log.error("\n--- Last peer status (res) ---")
                log.error(last_exc.res)

            raise AssertionError(
                f"{func.__name__} did not succeed within {timeout}s "
                f"after {attempt - 1} attempts"
            ) from last_exc

        return wrapper
    return decorator


class TestMirroring(CephFSTestCase):
    MDSS_REQUIRED = 5
    CLIENTS_REQUIRED = 2
    REQUIRE_BACKUP_FILESYSTEM = True

    MODULE_NAME = "mirroring"

    PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR = "cephfs_mirror"
    PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS = "cephfs_mirror_mirrored_filesystems"
    PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_PEER = "cephfs_mirror_peers"
    MIRROR_TICK_INTERVAL = 1

    def setUp(self):
        super(TestMirroring, self).setUp()
        self.primary_fs_name = self.fs.name
        self.primary_fs_id = self.fs.id
        self.secondary_fs_name = self.backup_fs.name
        self.secondary_fs_id = self.backup_fs.id
        self.enable_mirroring_module()
        self.config_set('client.mirror', 'cephfs_mirror_directory_scan_interval', 1)
        self.config_set('client.mirror', 'cephfs_mirror_tick_interval',
                          self.MIRROR_TICK_INTERVAL)

    def tearDown(self):
        self.disable_mirroring_module()
        super(TestMirroring, self).tearDown()

    def enable_mirroring_module(self):
        self.run_ceph_cmd("mgr", "module", "enable", TestMirroring.MODULE_NAME)

    def disable_mirroring_module(self):
        self.run_ceph_cmd("mgr", "module", "disable", TestMirroring.MODULE_NAME)

    def enable_mirroring(self, fs_name, fs_id):
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.run_ceph_cmd("fs", "snapshot", "mirror", "enable", fs_name)
        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        self.assertTrue(res['peers'] == {})
        self.assertTrue(res['snap_dirs']['dir_count'] == 0)

        # verify labelled perf counter
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        self.assertEqual(res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]["labels"]["filesystem"],
                         fs_name)
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.assertGreater(vafter["counters"]["mirrored_filesystems"],
                           vbefore["counters"]["mirrored_filesystems"])

    def disable_mirroring(self, fs_name, fs_id):
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.run_ceph_cmd("fs", "snapshot", "mirror", "disable", fs_name)
        time.sleep(10)
        # verify via asok
        try:
            self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                       'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        except CommandFailedError:
            pass
        else:
            raise RuntimeError('expected admin socket to be unavailable')

        # verify labelled perf counter
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.assertLess(vafter["counters"]["mirrored_filesystems"],
                        vbefore["counters"]["mirrored_filesystems"])

    def verify_peer_added(self, fs_name, fs_id, peer_spec, remote_fs_name=None):
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        peer_uuid = self.get_peer_uuid(peer_spec)
        self.assertTrue(peer_uuid in res['peers'])
        client_name = res['peers'][peer_uuid]['remote']['client_name']
        cluster_name = res['peers'][peer_uuid]['remote']['cluster_name']
        self.assertTrue(peer_spec == f'{client_name}@{cluster_name}')
        if remote_fs_name:
            self.assertTrue(self.secondary_fs_name == res['peers'][peer_uuid]['remote']['fs_name'])
        else:
            self.assertTrue(self.fs_name == res['peers'][peer_uuid]['remote']['fs_name'])

    def peer_add(self, fs_name, fs_id, peer_spec, remote_fs_name=None, check_perf_counter=True):
        if check_perf_counter:
            res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        if remote_fs_name:
            self.run_ceph_cmd("fs", "snapshot", "mirror", "peer_add", fs_name, peer_spec, remote_fs_name)
        else:
            self.run_ceph_cmd("fs", "snapshot", "mirror", "peer_add", fs_name, peer_spec)
        time.sleep(10)
        self.verify_peer_added(fs_name, fs_id, peer_spec, remote_fs_name)

        if check_perf_counter:
            res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]
            self.assertGreater(vafter["counters"]["mirroring_peers"], vbefore["counters"]["mirroring_peers"])

    def add_directory(self, fs_name, fs_id, dir_name, check_perf_counter=True):
        if check_perf_counter:
            res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        # get initial dir count
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        dir_count = res['snap_dirs']['dir_count']
        log.debug(f'initial dir_count={dir_count}')

        self.run_ceph_cmd("fs", "snapshot", "mirror", "add", fs_name, dir_name)

        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        new_dir_count = res['snap_dirs']['dir_count']
        log.debug(f'new dir_count={new_dir_count}')
        self.assertTrue(new_dir_count > dir_count)

        if check_perf_counter:
            res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]
            self.assertGreater(vafter["counters"]["directory_count"], vbefore["counters"]["directory_count"])

    def peer_dir_status(self, res, dir_name, peer_uuid):
        self.assertIn('metrics', res)
        return res['metrics'][dir_name]['peer'][peer_uuid]

    @retry_assert(timeout=600, interval=10)
    def check_peer_status(self, fs_name, fs_id, peer_spec, dir_name, expected_snap_name,
                          expected_snap_count):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                         'fs', 'mirror', 'peer', 'status',
                                         f'{fs_name}@{fs_id}', peer_uuid)
        try:
            dir_stat = self.peer_dir_status(res, dir_name, peer_uuid)
            self.assertTrue(dir_stat['last_synced_snap']['name'] == expected_snap_name)
            self.assertTrue(dir_stat['snaps_synced'] == expected_snap_count)
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

    def verify_snapshot(self, dir_name, snap_name):
        snap_list = self.mount_b.ls(path=f'{dir_name}/.snap')
        self.assertTrue(snap_name in snap_list)

        source_res = self.mount_a.dir_checksum(path=f'{dir_name}/.snap/{snap_name}',
                                               follow_symlinks=True)
        log.debug(f'source snapshot checksum {snap_name} {source_res}')

        dest_res = self.mount_b.dir_checksum(path=f'{dir_name}/.snap/{snap_name}',
                                           follow_symlinks=True)
        log.debug(f'destination snapshot checksum {snap_name} {dest_res}')
        self.assertTrue(source_res == dest_res)

    def get_peer_uuid(self, peer_spec):
        status = self.fs.status()
        fs_map = status.get_fsmap_byname(self.primary_fs_name)
        peers = fs_map['mirror_info']['peers']
        for peer_uuid, mirror_info in peers.items():
            client_name = mirror_info['remote']['client_name']
            cluster_name = mirror_info['remote']['cluster_name']
            remote_peer_spec = f'{client_name}@{cluster_name}'
            if peer_spec == remote_peer_spec:
                return peer_uuid
        return None

    def get_daemon_admin_socket(self):
        """overloaded by teuthology override (fs/mirror/clients/mirror.yaml)"""
        return "/var/run/ceph/cephfs-mirror.asok"

    def mirror_daemon_command(self, cmd_label, *args):
        asok_path = self.get_daemon_admin_socket()
        try:
            # use mount_a's remote to execute command
            p = self.mount_a.client_remote.run(args=
                     ['ceph', '--admin-daemon', asok_path] + list(args),
                     stdout=StringIO(), stderr=StringIO(), timeout=30,
                     check_status=True, label=cmd_label)
            p.wait()
        except CommandFailedError as ce:
            log.warn(f'mirror daemon command with label "{cmd_label}" failed: {ce}')
            raise
        res = p.stdout.getvalue().strip()
        log.debug(f'command returned={res}')
        return json.loads(res)

    def setup_mount_b(self, mds_perm):
        log.debug('reconfigure client auth caps')
        self.get_ceph_cmd_result(
            'auth', 'caps', f"client.{self.mount_b.client_id}",
            'mds', f'allow {mds_perm}',
            'mon', 'allow r',
            'osd', f"allow rw pool={self.backup_fs.get_data_pool_name()}")
        self.mount_b.umount_wait()
        log.debug(f'mounting filesystem {self.secondary_fs_name}')
        self.mount_b.mount_wait(cephfs_name=self.secondary_fs_name)

    def test_cephfs_mirror_incremental_sync(self):
        """ Test incremental snapshot synchronization (based on mtime differences)."""

        self.setup_mount_b(mds_perm='rw')
        repo = 'ceph-qa-suite'
        repo_dir = 'ceph_repo'
        repo_path = f'{repo_dir}/{repo}'

        def clone_repo():
            self.mount_a.run_shell([
                'git', 'clone', '--branch', 'giant',
                f'http://github.com/ceph/{repo}', repo_path])

        def exec_git_cmd(cmd_list):
            self.mount_a.run_shell(['git', '--git-dir', f'{self.mount_a.mountpoint}/{repo_path}/.git', *cmd_list])

        self.mount_a.run_shell(["mkdir", repo_dir])
        clone_repo()

        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)

        self.add_directory(self.primary_fs_name, self.primary_fs_id, f'/{repo_path}')
        # dump perf counters
        res = self.mirror_daemon_command(f'counter dump for fs: {self.primary_fs_name}', 'counter', 'dump')
        vfirst = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_PEER][0]
        self.mount_a.run_shell(['mkdir', f'{repo_path}/.snap/snap_a'])

        # full copy, takes time
        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               "client.mirror_remote@ceph", f'/{repo_path}', 'snap_a', 1)
        self.verify_snapshot(repo_path, 'snap_a')
        # check snaps_synced
        res = self.mirror_daemon_command(f'counter dump for fs: {self.primary_fs_name}', 'counter', 'dump')
        vsecond = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_PEER][0]
        self.assertGreater(vsecond["counters"]["snaps_synced"], vfirst["counters"]["snaps_synced"])
        full_sync_duration = vsecond["counters"]["last_synced_duration"]

        # create some diff
        num = random.randint(5, 10)
        log.debug(f'resetting to HEAD~{num}')
        exec_git_cmd(["reset", "--hard", f'HEAD~{num}'])

        self.mount_a.run_shell(['mkdir', f'{repo_path}/.snap/snap_b'])
        # incremental copy, should be fast
        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               "client.mirror_remote@ceph", f'/{repo_path}', 'snap_b', 2)
        self.verify_snapshot(repo_path, 'snap_b')
        res = self.mirror_daemon_command(f'counter dump for fs: {self.primary_fs_name}', 'counter', 'dump')
        vthird = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_PEER][0]
        self.assertGreater(vthird["counters"]["snaps_synced"], vsecond["counters"]["snaps_synced"])
        inc_sync_duration1 = vthird["counters"]["last_synced_duration"]
        log.debug(f'HRK full_sync_duration - {full_sync_duration}, inc_sync_duration1 - {inc_sync_duration1}')
        # self.assertGreaterEqual(float(full_sync_duration), float(inc_sync_duration1))

        # diff again, this time back to HEAD
        log.debug('resetting to HEAD')
        exec_git_cmd(["pull"])

        self.mount_a.run_shell(['mkdir', f'{repo_path}/.snap/snap_c'])
        # incremental copy, should be fast
        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               "client.mirror_remote@ceph", f'/{repo_path}', 'snap_c', 3)
        self.verify_snapshot(repo_path, 'snap_c')
        res = self.mirror_daemon_command(f'counter dump for fs: {self.primary_fs_name}', 'counter', 'dump')
        vfourth = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_PEER][0]
        self.assertGreater(vfourth["counters"]["snaps_synced"], vthird["counters"]["snaps_synced"])
        inc_sync_duration2 = vfourth["counters"]["last_synced_duration"]
        log.debug(f'HRK full_sync_duration - {full_sync_duration}, inc_sync_duration2 - {inc_sync_duration2}')
        # self.assertGreaterEqual(float(full_sync_duration), float(inc_sync_duration2))

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_cephfs_mirror_sync_with_purged_snapshot(self):
        """Test snapshot synchronization in midst of snapshot deletes.

        Deleted the previous snapshot when the mirror daemon is figuring out
        incremental differences between current and previous snapshot. The
        mirror daemon should identify the purge and switch to using remote
        comparison to sync the snapshot (in the next iteration of course).
        """

        self.setup_mount_b(mds_perm='rw')
        repo = 'ceph-qa-suite'
        repo_dir = 'ceph_repo'
        repo_path = f'{repo_dir}/{repo}'

        def clone_repo():
            self.mount_a.run_shell([
                'git', 'clone', '--branch', 'giant',
                f'http://github.com/ceph/{repo}', repo_path])

        def exec_git_cmd(cmd_list):
            self.mount_a.run_shell(['git', '--git-dir', f'{self.mount_a.mountpoint}/{repo_path}/.git', *cmd_list])

        self.mount_a.run_shell(["mkdir", repo_dir])
        clone_repo()

        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)

        self.add_directory(self.primary_fs_name, self.primary_fs_id, f'/{repo_path}')
        # dump perf counters
        res = self.mirror_daemon_command(f'counter dump for fs: {self.primary_fs_name}', 'counter', 'dump')
        vfirst = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_PEER][0]
        self.mount_a.run_shell(['mkdir', f'{repo_path}/.snap/snap_a'])

        # full copy, takes time
        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               "client.mirror_remote@ceph", f'/{repo_path}', 'snap_a', 1)
        self.verify_snapshot(repo_path, 'snap_a')
        res = self.mirror_daemon_command(f'counter dump for fs: {self.primary_fs_name}', 'counter', 'dump')
        vsecond = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_PEER][0]
        self.assertGreater(vsecond["counters"]["snaps_synced"], vfirst["counters"]["snaps_synced"])

        # create some diff
        num = random.randint(60, 100)
        log.debug(f'resetting to HEAD~{num}')
        exec_git_cmd(["reset", "--hard", f'HEAD~{num}'])

        self.mount_a.run_shell(['mkdir', f'{repo_path}/.snap/snap_b'])

        time.sleep(15)
        self.mount_a.run_shell(['rmdir', f'{repo_path}/.snap/snap_a'])

        # incremental copy but based on remote dir_root
        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               "client.mirror_remote@ceph", f'/{repo_path}', 'snap_b', 2)
        self.verify_snapshot(repo_path, 'snap_b')
        res = self.mirror_daemon_command(f'counter dump for fs: {self.primary_fs_name}', 'counter', 'dump')
        vthird = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_PEER][0]
        self.assertGreater(vthird["counters"]["snaps_synced"], vsecond["counters"]["snaps_synced"])

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)
