#!/bin/sh -e

# perform a release, to be executed on travis-ci.org

if [ -z "$DOCKERHUB_PASSWORD" ]; then
  echo "This is supposed to be executed on travis. Use .travis/release.sh to trigger a release job."
  exit 1
fi

echo "Performing docker login"
echo $DOCKERHUB_PASSWORD | docker login -u $DOCKERHUB_USERNAME --password-stdin

echo "Importing GPG keys"
echo $GPG_SECRET_KEYS | base64 --decode | gpg --import
echo $GPG_OWNERTRUST | base64 --decode | gpg --import-ownertrust

echo "Checkout master branch explicitly, as we run the release with a in detached head."
git checkout -qf master;

echo "Starting Maven release"
mvn --settings ./.travis/settings.xml release:prepare release:perform
