#!/bin/sh -x

set -e

ceph='bin/ceph'
fs='a'

$ceph fs subvolume create $fs sub_0
subvol_path=$($ceph fs subvolume getpath $fs sub_0 2>dev/null)

bin/ceph-fuse -c ./ceph.conf /mnt
dd if=/dev/urandom of=/mnt/$subvol_path/5GB_file-1 status=progress bs=1M count=5000

$ceph osd set-full-ratio 0.2
$ceph osd set-nearfull-ratio 0.16
$ceph osd set-backfillfull-ratio 0.18

$ceph osd df
#Sleep 5 seconds to reflect osd config settings
sleep 5

$ceph fs subvolume rm $fs sub_0
rm -f /mnt/$subvol_path/5GB_file-1
umount /mnt

echo OK
