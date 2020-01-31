* `mvn versions:set -DnewVersion=2.1.19-datadobi-X`
* `mvn clean`
* `mvn deploy -Durl=https://mavenserver.dobi:8081/artifactory/libs-release-local -DrepositoryId=datadobi`
* `mvn versions:set -DnewVersion=2.1.19-datadobi-(X+1)-SNAPSHOT`
