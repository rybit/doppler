#! /bin/bash

# is the dir clean?
git diff-index --quiet HEAD --
[ $? -eq 0 ] || { echo "Must release from a clean directory"; exit 1; }

# is this a tagged build?
tag=$(git tag --points-at $(git rev-parse HEAD))

# is it a valid tag?
[[ "$tag" =~ ^v[0-9.*].[0-9.*].[\-_0-9a-z.*] ]] || { echo "The tag '$tag' must be a semantic version"; exit 1; }

make build_linux
[ $? -eq 0 ] || { echo "failed to build"; exit 1; }

tar -czf doppler_linux_amd64_$tag.tar.gz doppler_linux_amd64 example.config.json
rm doppler_linux_amd64

