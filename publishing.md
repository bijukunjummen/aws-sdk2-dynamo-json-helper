# Publishing to Maven Central
The released artifacts are published to Maven central via Sonatype repository, the details of how the priveleges were obtained is here

To release from your desktop first add the credentials related to Sonatype(ossrh) in a gradle.properties file in your ~/.gradle directory, note that this should be totally private to your machine and should never be shared.

```
signing.keyId=GPG_KEY_ID
signing.password=GPG_PASSWORD
signing.secretKeyRingFile=Signing Key Ring..

ossrhUsername=user
ossrhPassword=password
```

If all of the above is cleanly set, the artifact can be published as a release candidate using the command:

```
./gradlew candidate
```
It takes about 10 mins before the artifacts are visible via maven and 2 hours before it is searchable.

For versioned releases:
go [here](https://oss.sonatype.org/#stagingRepositories)

search for comgithubbijukunjummen

"Close" the release

Then click on "Release