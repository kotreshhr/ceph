import os

from .group import Group


def splitall(path):
    if path == "/":
        return ["/"]
    s = os.path.split(path)
    return splitall(s[0]) + [s[1]]


def resolve(vol_spec, path):
    parts = splitall(path)
    if len(parts) != 4 or os.path.join(parts[0], parts[1]) != vol_spec.subvolume_prefix:
        return None
    groupname = None if parts[2] == Group.NO_GROUP_NAME else parts[2]
    subvolname = parts[3]
    return (groupname, subvolname)


def resolve_trash(vol_spec, path):
    parts = splitall(path)

    # This is to resolve the trashpath of subvolumes that retain snapshots.
    # The subvolume trash path has the following syntax.
    # /<vol_spec.subvolume_prefix>/<groupname>/<subvolname>/.trash/<subvolUUID> ?
    if len(parts) == 6:
        if os.path.join(parts[0], parts[1]) != vol_spec.subvolume_prefix or \
           parts[4] != '.trash':
            return None
        groupname = None if parts[2] == Group.NO_GROUP_NAME else parts[2]
        subvolname = parts[3]
        return (groupname, subvolname)
    # This is to resolve the trashpath of subvolumes which belong to subvolume groups
    # with quota. The subvolume trash path has the following syntax.
    # /<vol_spec.subvolume_prefix>/<groupname>/.trash/<subvolname>
    elif len(parts) == 5:
        if os.path.join(parts[0], parts[1]) != vol_spec.subvolume_prefix or \
           parts[3] != '.trash':
            return None
        groupname = None if parts[2] == Group.NO_GROUP_NAME else parts[2]
        subvolname = parts[4]
        return (groupname, subvolname)
    else:
        return None
