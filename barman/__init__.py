#
# barman - Backup and Recovery Manager for PostgreSQL
#
# Copyright (C) 2011  2ndQuadrant Italia (Devise.IT S.r.l.) <info@2ndquadrant.it>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from version import __version__

__config__ = None

def _pretty_size(size, unit=1024):
    SUFFIXES = ["B"] + [i + {1000: "B", 1024: "iB"}[unit] for i in "KMGTPEZY"]
    for suffix in SUFFIXES:
        if size < unit or suffix == SUFFIXES[-1]:
            if suffix == SUFFIXES[0]:
                return "%d %s" % (size, suffix)
            else:
                return "%.1f %s" % (size, suffix)
        else:
            size /= unit
