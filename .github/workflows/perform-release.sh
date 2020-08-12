#!/bin/sh -e

# perform a release, to be executed as github action

if [ -z "$DOCKERHUB_PASSWORD" ]; then
  echo "This is supposed to be executed as github action."
  exit 1
fi

echo "Performing docker login..."
echo $DOCKERHUB_PASSWORD | docker login -u $DOCKERHUB_USERNAME --password-stdin

echo "Importing GPG keys..."
echo $GPG_SECRET_KEYS | base64 --decode | gpg --import --no-tty --batch --yes
echo $GPG_OWNERTRUST | base64 --decode | gpg --import-ownertrust --no-tty --batch --yes

echo "Configure git for the release commits"
git config user.email "Grp-opensourceoffice@adobe.com"
git config user.name "${X_GITHUB_USERNAME}"

echo "Starting Maven release..."
mvn --settings ./.github/workflows/settings.xml release:prepare release:perform
