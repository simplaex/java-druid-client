test: clean
	mvn verify

clean:
	mvn clean
	rm -rf coverage

publish: clean
	mvn deploy

purge: clean
	rm -rf .idea *.iml pom.xml.releaseBackup release.properties
