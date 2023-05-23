/*
 * This software is in the public domain under CC0 1.0 Universal plus a 
 * Grant of Patent License.
 * 
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software (see the LICENSE.md file). If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package org.moqui.aws

import groovy.transform.CompileStatic
import org.moqui.context.ExecutionContextFactory
import org.moqui.context.ToolFactory
import org.moqui.util.SystemBinding
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3ClientBuilder
import software.amazon.awssdk.services.s3.S3Client

/** A ToolFactory for AWS S3 Client */
@CompileStatic
class S3ClientToolFactory implements ToolFactory<S3Client> {
    protected final static Logger logger = LoggerFactory.getLogger(S3ClientToolFactory.class)
    final static String TOOL_NAME = "AwsS3Client"

    protected ExecutionContextFactory ecf = null
    protected S3Client s3Client = null

    /** Default empty constructor */
    S3ClientToolFactory() { }

    @Override String getName() { return TOOL_NAME }

    @Override
    void init(ExecutionContextFactory ecf) {
        this.ecf = ecf
        // NOTE: minimal explicit configuration here, see:
        // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html
        // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html

        // There is no Java sys prop key for region, and env var vs Java sys prop keys are different for access key ID and secret
        //     so normalize here to the standard SDK env var keys and support from Java sys props as well
        String awsRegion = SystemBinding.getPropOrEnv("AWS_REGION")
        String awsAccessKeyId = SystemBinding.getPropOrEnv("AWS_ACCESS_KEY_ID")
        String awsSecret = SystemBinding.getPropOrEnv("AWS_SECRET_ACCESS_KEY")
        String awsContainerCredentialsRelativeURI = SystemBinding.getPropOrEnv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
        String awsContainerCredentialsFullURI = SystemBinding.getPropOrEnv("AWS_CONTAINER_CREDENTIALS_FULL_URI")

        // Non standard AWS, for example Minio.
        String awsEndpointURL = SystemBinding.getPropOrEnv("AWS_ENDPOINT_URL")
        if (awsAccessKeyId && awsSecret) {
            System.setProperty("aws.accessKeyId", awsAccessKeyId)
            System.setProperty("aws.secretAccessKey", awsSecret)
        }

        logger.info("Starting AWS S3 Client with region ${awsRegion} access ID ${awsAccessKeyId}")

        S3ClientBuilder cb = S3Client.builder()
        if (awsRegion) cb.region(Region.of(awsRegion))
        if (awsEndpointURL) cb.endpointOverride(new URI(awsEndpointURL))
        //if (awsContainerCredentialsRelativeURI || awsContainerCredentialsFullURI) cb.credentialsProvider(ContainerCredentialsProvider.builder().build())
        s3Client = cb.build()
    }

    @Override S3Client getInstance(Object... parameters) { return s3Client }

    @Override
    void destroy() {
        // stop client to prevent more calls coming in
        if (s3Client != null) try {
            s3Client.close()
            logger.info("AWS S3 Client closed")
        } catch (Throwable t) { logger.error("Error in AWS S3 Client close", t) }
    }
}
