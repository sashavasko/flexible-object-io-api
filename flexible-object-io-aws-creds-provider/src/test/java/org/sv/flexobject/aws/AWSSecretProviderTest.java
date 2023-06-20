package org.sv.flexobject.aws;

import org.junit.Ignore;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertEquals;

public class AWSSecretProviderTest {

    /**
     * To run this test first do that in terminal :
     * vault login -method=okta username=$USER totp=NUMBERINOCTAVERIFY
     * tmpfile=$(mktemp /tmp/aws-login.XXXXXX)
     * vault read aws/development/<department>/sts/<role> > ${tmpfile}
     * export AWS_ACCESS_KEY_ID=`grep access_key ${tmpfile} | awk '{print $2}'`
     * export AWS_SECRET_ACCESS_KEY=`grep secret_key ${tmpfile} | awk '{print $2}'`
     * export AWS_SESSION_TOKEN=`grep security_token ${tmpfile} | awk '{print $2}'`
     * aws secretsmanager get-secret-value --secret-id arn:aws:secretsmanager:us-east-1:number1:secret:path/to/secret --region us-east-1
     *
     * Use values of AWS env vars to populate credentials in the test
     * Use value of SecretString.password from this last command to populate expected password
     */

    @Ignore
    @Test
    public void testMongoReadonlyForReal() {

        String accessKeyId = "";
        String secretAccessKey = "";
        String sessionToken = "";
        String expectedPassword = "";
        String arn = "arn:aws:secretsmanager:us-east-1:number:secret:path/to/secret";

        if (secretAccessKey.isEmpty() || sessionToken.isEmpty() || expectedPassword.isEmpty())
            throw new RuntimeException("Please fill in the values according to instructions");

        AwsCredentials credentials = AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
        AWSSecretProvider provider = new AWSSecretProvider() {
            @Override
            public AwsCredentialsProvider getCredentialsProvider(String awsLevel) {
                return StaticCredentialsProvider.create(credentials);
            }
        };

        String secret = provider.getAWSSecret(arn, Region.US_EAST_1, "staging");
        assertEquals(expectedPassword, secret);
    }
}