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

import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CommonPrefix
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.ObjectVersion
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.S3Object

import groovy.transform.CompileStatic

import org.moqui.BaseArtifactException
import org.moqui.BaseException
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.context.reference.BaseResourceReference
import org.moqui.resource.ResourceReference
// NOTE: IDE says this isn't needed but compiler requires it
import org.moqui.resource.ResourceReference.Version
import org.moqui.util.ObjectUtilities

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Timestamp

// TODO: need to worry about ResetException? would have to use temp files for all puts, see https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/best-practices.html

/*
Handy Docs:
https://docs.aws.amazon.com/S3Client/latest/dev/UsingTheMPJavaAPI.html
https://docs.aws.amazon.com/S3Client/latest/dev/ObjectOperations.html
https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-s3-objects.html

Important Classes:
https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/S3Client.html
https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/S3Object.html
https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/ObjectMetadata.html
 */

@CompileStatic
class S3ResourceReference extends BaseResourceReference {
    protected final static Logger logger = LoggerFactory.getLogger(S3ResourceReference.class)
    public final static String locationPrefix = "aws3://"
    public final static boolean autoCreateBucket = true

    // don't static init this, just in case inits before ExecutionContextFactoryImpl inits and sets default properties
    private static Map<String, String> bucketAliasMapInternal = null
    static Map<String, String> getBucketAliasMap() {
        if (bucketAliasMapInternal != null) return bucketAliasMapInternal
        Map<String, String> tempAliasMap = new HashMap<>()
        for (int i = 1; i < 9; i++) {
            String alias = System.getProperty("aws_s3_bucket_alias" + i)
            String name = System.getProperty("aws_s3_bucket_name" + i)
            if (alias && name) {
                tempAliasMap.put(alias, name)
            } else {
                alias = System.getenv("aws_s3_bucket_alias" + i)
                name = System.getenv("aws_s3_bucket_name" + i)
                if (alias && name) tempAliasMap.put(alias, name)
            }
        }
        bucketAliasMapInternal = tempAliasMap
        return bucketAliasMapInternal
    }

    String location
    Boolean knownDirectory = (Boolean) null

    S3ResourceReference() { }

    @Override ResourceReference init(String location, ExecutionContextFactoryImpl ecf) {
        this.ecf = ecf
        this.location = location
        return this
    }
    S3ResourceReference init(String location, Boolean knownDirectory, ExecutionContextFactoryImpl ecf) {
        this.ecf = ecf
        this.location = location
        this.knownDirectory = knownDirectory
        return this
    }

    @Override ResourceReference createNew(String location) {
        S3ResourceReference resRef = new S3ResourceReference()
        resRef.init(location, ecf)
        return resRef
    }
    @Override String getLocation() { location }

    static String getBucketName(String location) {
        if (!location) throw new BaseArtifactException("No location specified, cannot get bucket name (first path segment)")
        // after prefix first path segment is bucket name
        String fullPath = location.substring(locationPrefix.length())
        int slashIdx = fullPath.indexOf("/")
        String bName = slashIdx == -1 ? fullPath : fullPath.substring(0, slashIdx)
        if (!bName) throw new BaseArtifactException("No bucket name (first path segment) in location ${location}")

        // see if bucket name is an alias
        Map<String, String> aliasMap = getBucketAliasMap()
        String aliasName = aliasMap.get(bName)
        if (aliasName != null && !aliasName.isEmpty()) bName = aliasName

        return bName
    }
    static String getPath(String location) {
        if (!location) return ""
        // after prefix first path segment is bucket name so strip that to get path
        String fullPath = location.substring(locationPrefix.length())
        int slashIdx = fullPath.indexOf("/")
        if (slashIdx) {
            return fullPath.substring(slashIdx + 1)
        } else {
            return ""
        }
    }

    @Override InputStream openStream() {
        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            GetObjectRequest objectRequest = (GetObjectRequest) GetObjectRequest.builder().bucket(bucketName).key(path).build()
            ResponseInputStream<GetObjectResponse> objectResponse = s3Client.getObject(objectRequest)
            return objectResponse
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                logger.warn("Not found (404) error in openStream for bucket ${bucketName} path ${path}: ${e.toString()}")
                return null
            } else { throw e }
        }
    }

    @Override OutputStream getOutputStream() {
        // TODO can support this?
        throw new UnsupportedOperationException("The getOutputStream method is not supported for s3 resources, use putStream() instead")
    }

    @Override String getText() {
        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            GetObjectRequest objectRequest = (GetObjectRequest) GetObjectRequest.builder().bucket(bucketName).key(path).build()
            ResponseBytes<GetObjectResponse> objectResponse = s3Client.getObjectAsBytes(objectRequest)
            return objectResponse.asUtf8String()
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                logger.warn("Not found (404) error in getText for bucket ${bucketName} path ${path}: ${e.toString()}")
                return null
            } else { throw e }
        }
    }

    @Override boolean supportsAll() { true }

    @Override boolean supportsUrl() { false }
    @Override URL getUrl() { return null }

    @Override boolean supportsDirectory() { true }
    @Override boolean isFile() {
        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        return doesObjectExist(s3Client, bucketName, path)
    }

    boolean doesObjectExist(S3Client s3Client, String bucketName, String path) {
        if (knownDirectory != null) return !knownDirectory.booleanValue()
        try {
            HeadObjectRequest objectRequest = (HeadObjectRequest) HeadObjectRequest.builder().bucket(bucketName).key(path).build()
            // if throws exception then does not exist
            s3Client.headObject(objectRequest)
            knownDirectory = Boolean.FALSE // known file
            return true
        } catch (S3Exception e) {
            if (e.statusCode() == 403) {
                logger.warn("Not authorized (403) error in isFile for bucket ${bucketName} path ${path}: ${e.toString()}")
                return false
            } else if (e.statusCode() == 404) {
                return false
            } else { throw e }
        }
    }

    /** handle directories by seeing if is a prefix with any files, limit 1 for efficiency */
    boolean doesPathExist(S3Client s3Client, String bucketName, String path) {
        if (knownDirectory != null) return knownDirectory.booleanValue()
        ListObjectsV2Request lor = (ListObjectsV2Request) ListObjectsV2Request.builder().bucket(bucketName).prefix(path)
                .delimiter("/").maxKeys(1).build()
        ListObjectsV2Response result = s3Client.listObjectsV2(lor)
        if (result.hasContents() || result.hasCommonPrefixes()) {
            knownDirectory = Boolean.TRUE
            return true
        } else {
            return false
        }
    }

    @Override boolean isDirectory() {
        // logger.warn("isDirectory loc ${location} knownDirectory ${knownDirectory}")
        if (knownDirectory != null) return knownDirectory.booleanValue()

        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)
        if (!path) return true // consider root a directory

        // how to determine? not exists (not an object/file) but has files in it
        if (doesObjectExist(s3Client, bucketName, path)) return false
        return doesPathExist(s3Client, bucketName, path)
    }
    @Override List<ResourceReference> getDirectoryEntries() {
        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        // logger.warn("getDirectoryEntries bucket ${bucketName} path ${path}")

        ListObjectsV2Request lor = (ListObjectsV2Request) ListObjectsV2Request.builder().bucket(bucketName).prefix(path + "/")
                .delimiter("/").build()
        ListObjectsV2Response result = s3Client.listObjectsV2(lor)
        // common prefixes (sub-directories)
        List<CommonPrefix> commonPrefixList = result.commonPrefixes()
        // objects (files in directory)
        List<S3Object> objectList = result.contents()
        // add to the list
        ArrayList<ResourceReference> entryList = new ArrayList<>(commonPrefixList.size() + objectList.size())
        for (CommonPrefix subDir in commonPrefixList)
            entryList.add(new S3ResourceReference().init(location + '/' + subDir.prefix(), Boolean.TRUE, ecf))
        // NOTE: consider using alias for bucketName instead of straight bucketName
        for (S3Object os in objectList)
            entryList.add(new S3ResourceReference().init(locationPrefix + bucketName + '/' + os.key(), Boolean.FALSE, ecf))
        // logger.warn("sub-dirs: ${commonPrefixList.join(', ')}")
        // logger.warn("files: ${objectList.collect({it.getKey()}).join(', ')}")
        // logger.warn("RR entries: ${entryList.collect({it.toString()}).join(', ')}")
        return entryList
    }

    @Override boolean supportsExists() { true }
    @Override boolean getExists() {
        // if knownDirectory not null exists, if true then directory if false then file
        if (knownDirectory != null) return true

        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        // first see if it's a file
        boolean existingFile = doesObjectExist(s3Client, bucketName, path)
        if (existingFile) return true

        return doesPathExist(s3Client, bucketName, path)
    }

    @Override boolean supportsLastModified() { true }
    @Override long getLastModified() {
        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            HeadObjectRequest objectRequest = (HeadObjectRequest) HeadObjectRequest.builder().bucket(bucketName).key(path).build()
            // if throws exception then does not exist
            HeadObjectResponse response = s3Client.headObject(objectRequest)
            return response.lastModified().toEpochMilli()
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                logger.warn("Not found (404) error in getLastModified for bucket ${bucketName} path ${path}: ${e.toString()}")
                return 0
            } else { throw e }
        }
    }

    @Override boolean supportsSize() { true }
    @Override long getSize() {
        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            HeadObjectRequest objectRequest = (HeadObjectRequest) HeadObjectRequest.builder().bucket(bucketName).key(path).build()
            // if throws exception then does not exist
            HeadObjectResponse response = s3Client.headObject(objectRequest)
            response.lastModified().toEpochMilli()
            return response.contentLength()
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                logger.warn("Not found (404) error in getSize for bucket ${bucketName} path ${path}: ${e.toString()}")
                return 0
            } else { throw e }
        }
    }

    void autoCreateBucket(S3Client s3Client, String bucketName) {
        try {
            // if throws exception then does not exist
            s3Client.headBucket((HeadBucketRequest) HeadBucketRequest.builder().bucket(bucketName).build())
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                // not found, create bucket

            } else {
                throw e
            }
        }
    }

    @Override boolean supportsWrite() { true }
    @Override void putText(String text) {
        // FUTURE: use diff from last version for text
        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        if (autoCreateBucket) autoCreateBucket(s3Client, bucketName)

        s3Client.putObject((PutObjectRequest) PutObjectRequest.builder().bucket(bucketName).key(path).build(), RequestBody.fromString(text))
    }
    @Override void putStream(InputStream stream) {
        if (stream == null) return
        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        if (autoCreateBucket) autoCreateBucket(s3Client, bucketName)

        // NOTE: can specify more options using ObjectMetadata
        // NOTE: return PutObjectResult with more info, including version/etc
        // requires content length, all we have is an InputStream (not resettable) so read into byte[]
        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        ObjectUtilities.copyStream(stream, baos)
        s3Client.putObject((PutObjectRequest) PutObjectRequest.builder().bucket(bucketName).key(path).build(), RequestBody.fromBytes(baos.toByteArray()))
    }

    @Override void move(String newLocation) {
        if (!newLocation) throw new BaseArtifactException("No location specified, not moving resource at ${getLocation()}")
        if (!newLocation.startsWith(locationPrefix))
            throw new BaseArtifactException("Location [${newLocation}] is not a s3 location, not moving resource at ${getLocation()}")

        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        String newBucketName = getBucketName(newLocation)
        String newPath = getPath(newLocation)

        if (bucketName == newBucketName && path == newPath) return

        try {
            if (autoCreateBucket && bucketName != newBucketName) autoCreateBucket(s3Client, newBucketName)

            // if this is a file move directly, if a directory move all files with its prefix
            if ((knownDirectory != null && !knownDirectory.booleanValue()) || doesObjectExist(s3Client, bucketName, path)) {
                // FUTURE: handle source version somehow, maybe different move or copy method? pass as third parameter to CopyObjectRequest constructor
                s3Client.copyObject((CopyObjectRequest) CopyObjectRequest.builder().copySource(bucketName + '/' + path)
                        .destinationBucket(newBucketName).destinationKey(newPath).build())
                s3Client.deleteObject((DeleteObjectRequest) DeleteObjectRequest.builder().bucket(bucketName).key(path).build())
            } else {
                ListObjectsV2Request lor = (ListObjectsV2Request) ListObjectsV2Request.builder().bucket(bucketName).prefix(path + "/")
                        .delimiter("/").build()
                ListObjectsV2Response result = s3Client.listObjectsV2(lor)
                // objects (files in directory and all sub-directories)
                List<S3Object> objectList = result.contents()
                for (S3Object s3os in objectList) {
                    String srcPath = s3os.key()
                    String destPath = srcPath.replace(path, newPath)
                    s3Client.copyObject((CopyObjectRequest) CopyObjectRequest.builder().copySource(bucketName + '/' + srcPath)
                            .destinationBucket(newBucketName).destinationKey(destPath).build())
                    s3Client.deleteObject((DeleteObjectRequest) DeleteObjectRequest.builder().bucket(bucketName).key(srcPath).build())
                }
            }
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                logger.warn("Not found (404) error in move for bucket ${bucketName} path ${path}: ${e.toString()}")
                throw new BaseArtifactException("Could not move, file not found for bucket ${bucketName} path ${path}")
            } else { throw e }
        }
    }

    @Override ResourceReference makeDirectory(String name) {
        // NOTE can make directory with no files in S3? seems no, directory is just a partial object key
        return new S3ResourceReference().init("${location}/${name}", ecf)
    }
    @Override ResourceReference makeFile(String name) {
        S3ResourceReference newRef = new S3ResourceReference()
        newRef.init("${location}/${name}", ecf)
        // TODO make empty file?
        return newRef
    }
    @Override boolean delete() {
        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        if (!doesObjectExist(s3Client, bucketName, path)) return false
        s3Client.deleteObject((DeleteObjectRequest) DeleteObjectRequest.builder().bucket(bucketName).key(path).build())
        return true
    }

    @Override boolean supportsVersion() { return true }
    @Override Version getVersion(String versionName) {
        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            HeadObjectRequest.Builder requestBuilder = HeadObjectRequest.builder().bucket(bucketName).key(path)
            if (versionName) requestBuilder.versionId(versionName)
            // if throws exception then does not exist
            HeadObjectResponse response = s3Client.headObject((HeadObjectRequest) requestBuilder.build())

            if (!response.versionId()) return null

            // TODO: use setUserMetadata(Map<String,String> userMetadata) and getUserMetadata() for userId, needs to be on app puts/etc
            // TODO: worth a separate request to try to get previousVersionName? doesn't seem to be easy way to do that either...
            return new Version(this, response.versionId(), null, null, new Timestamp(response.lastModified().toEpochMilli()))
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                logger.warn("Not found (404) error in getVersion for bucket ${bucketName} path ${path}: ${e.toString()}")
                return null
            } else { throw e }
        }
    }
    @Override Version getCurrentVersion() { return getVersion(null) }
    @Override Version getRootVersion() {
        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            // NOTE: assuming this does oldest first, needs testing, docs not clear on any of this stuff
            ListObjectVersionsRequest request = (ListObjectVersionsRequest) ListObjectVersionsRequest.builder().bucket(bucketName)
                    .prefix(path).maxKeys(1).build()
            ListObjectVersionsResponse response = s3Client.listObjectVersions(request)
            List<ObjectVersion> versionList = response.versions()
            if (versionList == null || versionList.size() == 0) return null
            ObjectVersion version = versionList.get(0)

            return new Version(this, version.versionId(), null, null, new Timestamp(version.lastModified().toEpochMilli()))
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                logger.warn("Not found (404) error in getRootVersion for bucket ${bucketName} path ${path}: ${e.toString()}")
                return null
            } else { throw e }
        }
    }
    @Override ArrayList<Version> getVersionHistory() {
        return getNextVersions(null)
    }
    @Override ArrayList<Version> getNextVersions(String versionName) {
        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        ListObjectVersionsRequest.Builder requestBuilder = ListObjectVersionsRequest.builder().bucket(bucketName).prefix(path)
        // NOTE: any way to get versions that have this versionName as the previous version? doesn't seem so, ie no branching just linear list so just get next version
        if (versionName != null && !versionName.isEmpty()) requestBuilder.versionIdMarker(versionName).maxKeys(1)

        try {
            ListObjectVersionsResponse response = s3Client.listObjectVersions((ListObjectVersionsRequest) requestBuilder.build())
            List<ObjectVersion> s3vsList = response.versions()

            ArrayList<Version> verList = new ArrayList<>(s3vsList.size())
            String prevVersion = null
            for (ObjectVersion s3vs in s3vsList) {
                verList.add(new Version(this, s3vs.versionId(), prevVersion, null, new Timestamp(s3vs.lastModified().toEpochMilli())))
                prevVersion = s3vs.versionId()
            }
            return verList
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                logger.warn("Not found (404) error in getNextVersions for bucket ${bucketName} path ${path} version ${versionName}: ${e.toString()}")
                return null
            } else { throw e }
        }
    }
    @Override InputStream openStream(String versionName) {
        if (versionName == null || versionName.isEmpty()) return openStream()

        S3Client s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            GetObjectRequest.Builder getBuilder = GetObjectRequest.builder().bucket(bucketName).key(path)
            if (versionName != null && !versionName.isEmpty()) getBuilder.versionId(versionName)
            ResponseInputStream<GetObjectResponse> response = s3Client.getObject((GetObjectRequest) getBuilder.build())
            return response
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                logger.warn("Not found (404) error in openStream for bucket ${bucketName} path ${path} version ${versionName}: ${e.toString()}")
                return null
            } else { throw e }
        }
    }
    @Override String getText(String versionName) { return ObjectUtilities.getStreamText(openStream(versionName)) }

    S3Client getS3Client() {
        S3Client s3Client = ecf.getTool(S3ClientToolFactory.TOOL_NAME, S3Client.class)
        if (s3Client == null) throw new BaseException("AWS S3 Client not initialized")
        return s3Client
    }
}
