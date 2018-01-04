#!/bin/sh -e

# trigger release script on travis-ci.org

echo "This will trigger a release job on Travis, building the SNAPSHOT version of master as a release and incrementing the new SNAPSHOT version by 0.0.1. Released artifacts cannot be removed from Maven Central. Are you sure you want to continue? (Y/n)"

read CONTINUE_RELEASE

if [ "$CONTINUE_RELEASE" = "Y" ]; then
  TRAVIS_TOKEN=$(travis token)
  TRAVIS_REQUEST='{
   "request": {
   "message": "Perform Maven Release",
   "branch":"master",
   "config": {
     "script": ".travis/travis-release.sh"
    }
  }}'

  curl -s -X POST \
   -H "Content-Type: application/json" \
   -H "Accept: application/json" \
   -H "Travis-API-Version: 3" \
   -H "Authorization: token $TRAVIS_TOKEN" \
   -d "$TRAVIS_REQUEST" \
   https://api.travis-ci.org/repo/adobe%2FS3Mock/requests
else
  echo "Aborted."
fi
