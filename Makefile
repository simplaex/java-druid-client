test: clean
	mvn -B verify

clean:
	mvn -B clean
	rm -rf coverage

publish: clean
	mvn -B -Prelease deploy

purge: clean
	rm -rf .idea *.iml pom.xml.releaseBackup release.properties
