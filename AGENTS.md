# Overview

Photohash is software used to manage collections of photos,
specifically by finding duplicates. For example, you might copy photos
from an iPhone or an SD card and not know whether you already have
them on your NAS.

It maintains an incrementally-indexed database of hashes. Directories
can be manually indexed with `photohash index <dir>`.

Then, `photohash diff` and `photohash separate` are used to find and
manage duplicates.

WARNING: `photohash separate` is a destructive operation -- never run
it yourself.

## Structure

The `s/` directory contains scripts used during development. For
example, `s/ci` is a full local CI run.

`s/lint` is a faster check and lint only.
