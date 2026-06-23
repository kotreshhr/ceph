import os
import json
import logging
import signal
import time
import functools

from io import StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while
from teuthology.orchestra import run

log = logging.getLogger(__name__)


RETRY_EXCEPTIONS = (AssertionError, KeyError, IndexError, CommandFailedError)


def retry_assert(timeout=60, interval=1):
    """Retry a test helper until assertions inside it pass or timeout expires."""
    tries = int(timeout / interval)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            attempt = 1

            with safe_while(sleep=interval, tries=tries,
                            action=f"retry {func.__name__}") as proceed:
                while proceed():
                    try:
                        return func(*args, **kwargs)
                    except RETRY_EXCEPTIONS as e:
                        last_exc = e
                        log.debug(
                            f"[retry_assert] {func.__name__}: "
                            f"attempt {attempt} failed ({type(e).__name__}), "
                            f"retrying...")
                        attempt += 1
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

    def setUp(self):
        super(TestMirroring, self).setUp()
        self.primary_fs_name = self.fs.name
        self.primary_fs_id = self.fs.id
        self.secondary_fs_name = self.backup_fs.name
        self.secondary_fs_id = self.backup_fs.id
        self.enable_mirroring_module()
        self.config_set('client.mirror', 'cephfs_mirror_directory_scan_interval', 1)
        self.config_set('client.mirror', 'cephfs_mirror_tick_interval', 1)

    def tearDown(self):
        self.disable_mirroring_module()
        super(TestMirroring, self).tearDown()

    def enable_mirroring_module(self):
        self.run_ceph_cmd("mgr", "module", "enable", TestMirroring.MODULE_NAME)

    def disable_mirroring_module(self):
        self.run_ceph_cmd("mgr", "module", "disable", TestMirroring.MODULE_NAME)

    def enable_mirroring(self, fs_name, fs_id):
        res = self.mirror_daemon_command(
            f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.run_ceph_cmd("fs", "snapshot", "mirror", "enable", fs_name)
        time.sleep(10)
        res = self.mirror_daemon_command(
            f'mirror status for fs: {fs_name}',
            'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        self.assertTrue(res['peers'] == {})
        self.assertTrue(res['snap_dirs']['dir_count'] == 0)

        res = self.mirror_daemon_command(
            f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]
        self.assertGreater(vafter["counters"]["mirrored_filesystems"],
                           vbefore["counters"]["mirrored_filesystems"])

    def disable_mirroring(self, fs_name, fs_id):
        res = self.mirror_daemon_command(
            f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.run_ceph_cmd("fs", "snapshot", "mirror", "disable", fs_name)
        time.sleep(10)
        try:
            self.mirror_daemon_command(
                f'mirror status for fs: {fs_name}',
                'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        except CommandFailedError:
            pass
        else:
            raise RuntimeError('expected admin socket to be unavailable')

        res = self.mirror_daemon_command(
            f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]
        self.assertLess(vafter["counters"]["mirrored_filesystems"],
                        vbefore["counters"]["mirrored_filesystems"])

    def verify_peer_added(self, fs_name, fs_id, peer_spec, remote_fs_name=None):
        res = self.mirror_daemon_command(
            f'mirror status for fs: {fs_name}',
            'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        peer_uuid = self.get_peer_uuid(peer_spec)
        self.assertTrue(peer_uuid in res['peers'])
        client_name = res['peers'][peer_uuid]['remote']['client_name']
        cluster_name = res['peers'][peer_uuid]['remote']['cluster_name']
        self.assertTrue(peer_spec == f'{client_name}@{cluster_name}')
        if remote_fs_name:
            self.assertTrue(
                self.secondary_fs_name ==
                res['peers'][peer_uuid]['remote']['fs_name'])

    def peer_add(self, fs_name, fs_id, peer_spec, remote_fs_name=None,
                 check_perf_counter=True):
        if check_perf_counter:
            res = self.mirror_daemon_command(
                f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        if remote_fs_name:
            self.run_ceph_cmd("fs", "snapshot", "mirror", "peer_add", fs_name,
                              peer_spec, remote_fs_name)
        else:
            self.run_ceph_cmd("fs", "snapshot", "mirror", "peer_add", fs_name,
                              peer_spec)
        time.sleep(10)
        self.verify_peer_added(fs_name, fs_id, peer_spec, remote_fs_name)

        if check_perf_counter:
            res = self.mirror_daemon_command(
                f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]
            self.assertGreater(vafter["counters"]["mirroring_peers"],
                               vbefore["counters"]["mirroring_peers"])

    def add_directory(self, fs_name, fs_id, dir_name, check_perf_counter=True):
        if check_perf_counter:
            res = self.mirror_daemon_command(
                f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        res = self.mirror_daemon_command(
            f'mirror status for fs: {fs_name}',
            'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        dir_count = res['snap_dirs']['dir_count']

        self.run_ceph_cmd("fs", "snapshot", "mirror", "add", fs_name, dir_name)
        time.sleep(10)

        res = self.mirror_daemon_command(
            f'mirror status for fs: {fs_name}',
            'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        self.assertTrue(res['snap_dirs']['dir_count'] > dir_count)

        if check_perf_counter:
            res = self.mirror_daemon_command(
                f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]
            self.assertGreater(vafter["counters"]["directory_count"],
                               vbefore["counters"]["directory_count"])

    def remove_directory(self, fs_name, fs_id, dir_name):
        res = self.mirror_daemon_command(
            f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        res = self.mirror_daemon_command(
            f'mirror status for fs: {fs_name}',
            'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        dir_count = res['snap_dirs']['dir_count']

        self.run_ceph_cmd("fs", "snapshot", "mirror", "remove", fs_name, dir_name)
        time.sleep(10)

        res = self.mirror_daemon_command(
            f'mirror status for fs: {fs_name}',
            'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        self.assertTrue(res['snap_dirs']['dir_count'] < dir_count)

        res = self.mirror_daemon_command(
            f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]
        self.assertLess(vafter["counters"]["directory_count"],
                        vbefore["counters"]["directory_count"])

    def peer_dir_status(self, res, dir_name, peer_uuid):
        self.assertIn('metrics', res)
        return res['metrics'][dir_name]['peer'][peer_uuid]

    def mgr_mirror_status(self, fs_name, mirrored_dir_path=None, peer_uuid=None):
        args = ["fs", "snapshot", "mirror", "status", fs_name]
        if mirrored_dir_path is not None:
            args.append(mirrored_dir_path)
        if peer_uuid is not None:
            args.append(f'--peer_uuid={peer_uuid}')
        return json.loads(self.get_ceph_cmd_stdout(*args))

    def peer_status(self, fs_name, fs_id, peer_uuid):
        return self.mirror_daemon_command(
            f'peer status for fs: {fs_name}',
            'fs', 'mirror', 'peer', 'status',
            f'{fs_name}@{fs_id}', peer_uuid)

    def dir_status_from_mgr(self, fs_name, dir_name, peer_uuid,
                            mirrored_dir_path=None):
        res = self.mgr_mirror_status(
            fs_name, mirrored_dir_path or dir_name, peer_uuid)
        return self.peer_dir_status(res, dir_name, peer_uuid)

    def dir_status_from_asok(self, fs_name, fs_id, dir_name, peer_uuid):
        res = self.peer_status(fs_name, fs_id, peer_uuid)
        return self.peer_dir_status(res, dir_name, peer_uuid)

    def wait_for_mirror_daemon_stop(self, pid):
        with safe_while(sleep=1, tries=60,
                        action='wait for mirror daemon stop') as proceed:
            while proceed():
                try:
                    cur_pid = self.get_mirror_daemon_pid()
                except CommandFailedError:
                    return
                if cur_pid != pid:
                    return
                p = self.mount_a.run_shell(['kill', '-0', cur_pid],
                                           check_status=False)
                if p.returncode != 0:
                    return

    def restart_mirror_daemon(self):
        daemons = list(self.ctx.daemons.iter_daemons_of_role('cephfs-mirror'))
        self.assertEqual(len(daemons), 1,
                         'expected a single cephfs-mirror daemon')
        daemon = daemons[0]
        rados_inst_before = self.get_mirror_rados_addr(
            self.primary_fs_name, self.primary_fs_id)
        pid = self.get_mirror_daemon_pid()

        log.debug(f'SIGTERM to cephfs-mirror pid {pid}')
        if daemon.running():
            try:
                daemon.signal(signal.SIGTERM, silent=True)
            except Exception as e:
                log.debug(f'failed to signal cephfs-mirror via teuthology: {e}')
        self.mount_a.run_shell(['kill', '-TERM', pid])
        self.wait_for_mirror_daemon_stop(pid)

        if daemon.running():
            try:
                run.wait([daemon.proc], timeout=10)
            except CommandFailedError:
                pass
        daemon.reset()

        log.debug('starting cephfs-mirror')
        daemon.start()

        with safe_while(sleep=2, tries=30,
                        action='wait for mirror daemon restart') as proceed:
            while proceed():
                try:
                    rados_inst = self.get_mirror_rados_addr(
                        self.primary_fs_name, self.primary_fs_id)
                    if rados_inst and rados_inst != rados_inst_before:
                        break
                except CommandFailedError:
                    pass

    def wait_for_mirror_daemon_recovery(self, fs_name, fs_id, dir_name, peer_uuid):
        with safe_while(sleep=2, tries=60,
                        action='wait for mirror daemon recovery') as proceed:
            while proceed():
                try:
                    if not self.get_mirror_rados_addr(fs_name, fs_id):
                        continue
                except CommandFailedError:
                    continue
                try:
                    mirror_res = self.mirror_daemon_command(
                        f'mirror status for fs: {fs_name}',
                        'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
                except CommandFailedError:
                    continue
                if peer_uuid not in mirror_res.get('peers', {}):
                    continue
                if mirror_res.get('snap_dirs', {}).get('dir_count', 0) < 1:
                    continue
                try:
                    peer_res = self.peer_status(fs_name, fs_id, peer_uuid)
                    self.peer_dir_status(peer_res, dir_name, peer_uuid)
                except RETRY_EXCEPTIONS:
                    continue
                return

    @retry_assert(timeout=120, interval=1)
    def check_mgr_and_asok_session_counters_zero(self, fs_name, fs_id, dir_name,
                                                 peer_uuid):
        mgr_stat = self.dir_status_from_mgr(fs_name, dir_name, peer_uuid)
        asok_stat = self.dir_status_from_asok(fs_name, fs_id, dir_name, peer_uuid)
        self.assertEqual(mgr_stat['state'], 'idle')
        self.assertEqual(asok_stat['state'], 'idle')
        for key in ('snaps_synced', 'snaps_deleted', 'snaps_renamed'):
            self.assertEqual(mgr_stat.get(key), 0, msg=f'mgr {key}')
            self.assertEqual(asok_stat.get(key), 0, msg=f'asok {key}')
        self.assertEqual(mgr_stat['last_synced_snap']['name'],
                         asok_stat['last_synced_snap']['name'])

    @retry_assert(timeout=90, interval=5)
    def check_mgr_dir_stat_stale(self, fs_name, dir_name, peer_uuid):
        mgr_stat = self.dir_status_from_mgr(fs_name, dir_name, peer_uuid)
        self.assertEqual(mgr_stat['state'], 'stale',
                         msg=f'unexpected mgr stat: {mgr_stat}')
        self.assertNotIn('current_syncing_snap', mgr_stat)

    @retry_assert(timeout=60, interval=5)
    def check_peer_status_idle(self, fs_name, fs_id, peer_spec, dir_name,
                               expected_snap_name, expected_snap_count):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(
            f'peer status for fs: {fs_name}',
            'fs', 'mirror', 'peer', 'status',
            f'{fs_name}@{fs_id}', peer_uuid)
        try:
            dir_stat = self.peer_dir_status(res, dir_name, peer_uuid)
            self.assertTrue('idle' == dir_stat['state'])
            self.assertTrue(
                expected_snap_name == dir_stat['last_synced_snap']['name'])
            self.assertTrue(expected_snap_count == dir_stat['snaps_synced'])
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

    @retry_assert(timeout=60, interval=2)
    def check_peer_status_deleted_snap(self, fs_name, fs_id, peer_spec, dir_name,
                                       expected_delete_count):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(
            f'peer status for fs: {fs_name}',
            'fs', 'mirror', 'peer', 'status',
            f'{fs_name}@{fs_id}', peer_uuid)
        try:
            dir_stat = self.peer_dir_status(res, dir_name, peer_uuid)
            self.assertTrue(dir_stat['snaps_deleted'] == expected_delete_count)
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

    @retry_assert(timeout=60, interval=2)
    def check_peer_status_renamed_snap(self, fs_name, fs_id, peer_spec, dir_name,
                                       expected_rename_count):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(
            f'peer status for fs: {fs_name}',
            'fs', 'mirror', 'peer', 'status',
            f'{fs_name}@{fs_id}', peer_uuid)
        try:
            dir_stat = self.peer_dir_status(res, dir_name, peer_uuid)
            self.assertTrue(dir_stat['snaps_renamed'] == expected_rename_count)
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

    @retry_assert(timeout=60, interval=1)
    def check_peer_snap_in_progress(self, fs_name, fs_id, peer_spec, dir_name,
                                    snap_name):
        peer_uuid = self.get_peer_uuid(peer_spec)
        try:
            res = self.mirror_daemon_command(
                f'peer status for fs: {fs_name}',
                'fs', 'mirror', 'peer', 'status',
                f'{fs_name}@{fs_id}', peer_uuid)
            dir_stat = self.peer_dir_status(res, dir_name, peer_uuid)
            self.assertTrue('syncing' == dir_stat['state'])
            self.assertTrue(dir_stat['current_syncing_snap']['name'] == snap_name)
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

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

    def get_mirror_daemon_pid(self):
        """pid file overloaded in fs/mirror/clients/mirror.yaml"""
        return self.mount_a.run_shell(
            ['cat', '/var/run/ceph/cephfs-mirror.pid']).stdout.getvalue().strip()

    def get_mirror_rados_addr(self, fs_name, fs_id):
        res = self.mirror_daemon_command(
            f'mirror status for fs: {fs_name}',
            'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        if 'rados_inst' in res:
            return res['rados_inst']

    def mirror_daemon_command(self, cmd_label, *args):
        asok_path = self.get_daemon_admin_socket()
        try:
            p = self.mount_a.client_remote.run(
                args=['ceph', '--admin-daemon', asok_path] + list(args),
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

    def test_mgr_snapshot_mirror_status_stale_after_daemon_stop(self):
        """Mgr status marks frozen omap syncing progress as stale."""
        self.setup_mount_b(mds_perm='rw')
        self.mount_a.run_shell(["mkdir", "d0"])
        for i in range(8):
            self.mount_a.write_n_mb(os.path.join('d0', f'file.{i}'), 1024)

        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.add_directory(self.primary_fs_name, self.primary_fs_id, '/d0')
        peer_spec = "client.mirror_remote@ceph"
        self.peer_add(self.primary_fs_name, self.primary_fs_id, peer_spec,
                      self.secondary_fs_name)

        self.mount_a.run_shell(["mkdir", "d0/.snap/snap0"])
        self.check_peer_snap_in_progress(self.primary_fs_name, self.primary_fs_id,
                                         peer_spec, '/d0', 'snap0')

        peer_uuid = self.get_peer_uuid(peer_spec)
        mgr_syncing = False
        with safe_while(sleep=2, tries=30,
                        action='wait for omap syncing metrics') as proceed:
            while proceed():
                mgr_stat = self.dir_status_from_mgr(
                    self.primary_fs_name, '/d0', peer_uuid)
                if mgr_stat.get('state') == 'syncing':
                    mgr_syncing = True
                    break
        self.assertTrue(
            mgr_syncing,
            'mgr never reported syncing before SIGSTOP; '
            f'last mgr stat: {mgr_stat}')

        pid = self.get_mirror_daemon_pid()
        log.debug(f'SIGSTOP to cephfs-mirror pid {pid}')
        self.mount_a.run_shell(['kill', '-SIGSTOP', pid])
        try:
            time.sleep(40)
            self.check_mgr_dir_stat_stale(
                self.primary_fs_name, '/d0', peer_uuid)
        finally:
            log.debug('SIGCONT to cephfs-mirror')
            self.mount_a.run_shell(['kill', '-SIGCONT', pid])

        time.sleep(60)
        with safe_while(sleep=2, tries=20,
                        action='wait for mirror daemon recovery after SIGSTOP') as proceed:
            while proceed():
                try:
                    if not self.get_mirror_rados_addr(self.primary_fs_name,
                                                       self.primary_fs_id):
                        continue
                except CommandFailedError:
                    continue
                try:
                    res = self.mirror_daemon_command(
                        f'mirror status for fs: {self.primary_fs_name}',
                        'fs', 'mirror', 'status',
                        f'{self.primary_fs_name}@{self.primary_fs_id}')
                except CommandFailedError:
                    continue
                if 'snap_dirs' in res:
                    break

        self.remove_directory(self.primary_fs_name, self.primary_fs_id, '/d0')
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_mgr_snapshot_mirror_status_survives_daemon_restart(self):
        """Mgr status keeps persisted last_synced_snap and resets session counters after restart."""
        self.setup_mount_b(mds_perm='rw')
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        peer_spec = "client.mirror_remote@ceph"
        self.peer_add(self.primary_fs_name, self.primary_fs_id, peer_spec,
                      self.secondary_fs_name)

        dir_name = 'mgr_restart_dir'
        self.mount_a.run_shell(['mkdir', dir_name])
        self.mount_a.create_n_files(f'{dir_name}/file', 3000, sync=True)
        self.add_directory(self.primary_fs_name, self.primary_fs_id, f'/{dir_name}')

        snap0 = 'snap0'
        self.mount_a.run_shell(['mkdir', f'{dir_name}/.snap/{snap0}'])
        self.check_peer_status_idle(self.primary_fs_name, self.primary_fs_id,
                                    peer_spec, f'/{dir_name}', snap0, 1)

        for i in range(5):
            self.mount_a.write_n_mb(os.path.join(dir_name, f'more_file.{i}'), 1)

        snap1 = 'snap1'
        self.mount_a.run_shell(['mkdir', f'{dir_name}/.snap/{snap1}'])
        self.check_peer_status_idle(self.primary_fs_name, self.primary_fs_id,
                                    peer_spec, f'/{dir_name}', snap1, 2)

        self.mount_a.run_shell(['rmdir', f'{dir_name}/.snap/{snap0}'])
        self.check_peer_status_deleted_snap(self.primary_fs_name, self.primary_fs_id,
                                            peer_spec, f'/{dir_name}', 1)
        snap_list = self.mount_b.ls(path=f'{dir_name}/.snap')
        self.assertNotIn(snap0, snap_list)

        snap2 = 'snap2'
        self.mount_a.run_shell(['mv', f'{dir_name}/.snap/{snap1}',
                                f'{dir_name}/.snap/{snap2}'])
        self.check_peer_status_renamed_snap(self.primary_fs_name, self.primary_fs_id,
                                            peer_spec, f'/{dir_name}', 1)
        snap_list = self.mount_b.ls(path=f'{dir_name}/.snap')
        self.assertNotIn(snap1, snap_list)
        self.assertIn(snap2, snap_list)
        self.check_peer_status_idle(self.primary_fs_name, self.primary_fs_id,
                                    peer_spec, f'/{dir_name}', snap2, 2)

        peer_uuid = self.get_peer_uuid(peer_spec)
        before = self.dir_status_from_mgr(
            self.primary_fs_name, f'/{dir_name}', peer_uuid)
        self.assertEqual(before['snaps_synced'], 2)
        self.assertEqual(before['snaps_deleted'], 1)
        self.assertEqual(before['snaps_renamed'], 1)
        self.assertEqual(before['last_synced_snap']['name'], snap2)

        self.restart_mirror_daemon()
        self.wait_for_mirror_daemon_recovery(
            self.primary_fs_name, self.primary_fs_id, f'/{dir_name}', peer_uuid)
        self.check_mgr_and_asok_session_counters_zero(
            self.primary_fs_name, self.primary_fs_id, f'/{dir_name}', peer_uuid)

        after = self.dir_status_from_mgr(
            self.primary_fs_name, f'/{dir_name}', peer_uuid)
        self.assertEqual(after['last_synced_snap']['name'],
                         before['last_synced_snap']['name'])
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)
