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

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.CopyObjectRequest
import com.amazonaws.services.s3.model.GetObjectMetadataRequest
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.ListVersionsRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.s3.model.S3VersionSummary
import com.amazonaws.services.s3.model.VersionListing

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
https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingTheMPJavaAPI.html
https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectOperations.html
https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-s3-objects.html

Important Classes:
https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html
https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/S3Object.html
https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/ObjectMetadata.html
 */

@CompileStatic
class S3ResourceReference extends BaseResourceReference {
    protected final static Logger logger = LoggerFactory.getLogger(S3ResourceReference.class)
    public final static String locationPrefix = "aws3://"
    public final static boolean autoCreateBucket = true

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
        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            S3Object obj = s3Client.getObject(bucketName, path)
            S3ObjectInputStream s3is = obj.getObjectContent()
            return s3is
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
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
        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            return s3Client.getObjectAsString(bucketName, path)
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
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
        if (knownDirectory != null) return !knownDirectory.booleanValue()
        // NOTE how to determine? if exists is file should do for now
        if (s3Client.doesObjectExist(getBucketName(location), getPath(location))) {
            knownDirectory = Boolean.FALSE
            return true
        } else {
            return false
        }
    }
    @Override boolean isDirectory() {
        // logger.warn("isDirectory loc ${location} knownDirectory ${knownDirectory}")
        if (knownDirectory != null) return knownDirectory.booleanValue()

        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)
        if (!path) return true // consider root a directory

        // how to determine? not exists but has files in it
        if (s3Client.doesObjectExist(bucketName, path)) {
            knownDirectory = Boolean.FALSE
            return false
        }

        ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(path).withDelimiter("/").withMaxKeys(1)
        ListObjectsV2Result result = s3Client.listObjectsV2(lor)
        if (result.getObjectSummaries() || result.getCommonPrefixes()) {
            knownDirectory = Boolean.TRUE
            return true
        } else {
            return false
        }
    }
    @Override List<ResourceReference> getDirectoryEntries() {
        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        // logger.warn("getDirectoryEntries bucket ${bucketName} path ${path}")

        ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(path + "/").withDelimiter("/")
        ListObjectsV2Result result = s3Client.listObjectsV2(lor)
        // common prefixes (sub-directories)
        List<String> commonPrefixList = result.getCommonPrefixes()
        // objects (files in directory)
        List<S3ObjectSummary> objectList = result.getObjectSummaries()
        // add to the list
        ArrayList<ResourceReference> entryList = new ArrayList<>(commonPrefixList.size() + objectList.size())
        for (String subDir in commonPrefixList)
            entryList.add(new S3ResourceReference().init(location + '/' + subDir, Boolean.TRUE, ecf))
        for (S3ObjectSummary os in objectList)
            entryList.add(new S3ResourceReference().init(locationPrefix + os.getBucketName() + '/' + os.getKey(), Boolean.FALSE, ecf))
        // logger.warn("sub-dirs: ${commonPrefixList.join(', ')}")
        // logger.warn("files: ${objectList.collect({it.getKey()}).join(', ')}")
        // logger.warn("RR entries: ${entryList.collect({it.toString()}).join(', ')}")
        return entryList
    }

    @Override boolean supportsExists() { true }
    @Override boolean getExists() {
        if (knownDirectory != null && knownDirectory.booleanValue()) return true

        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        // first see if it's a file
        boolean existingFile = s3Client.doesObjectExist(bucketName, path)
        if (existingFile) {
            knownDirectory = Boolean.FALSE // known file
            return true
        }

        // handle directories by seeing if is a prefix with any files, limit 1 for efficiency
        ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(path).withDelimiter("/").withMaxKeys(1)
        ListObjectsV2Result result = s3Client.listObjectsV2(lor)
        if (result.getObjectSummaries() || result.getCommonPrefixes()) {
            knownDirectory = Boolean.TRUE
            return true
        } else {
            return false
        }
    }

    @Override boolean supportsLastModified() { true }
    @Override long getLastModified() {
        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            ObjectMetadata om = s3Client.getObjectMetadata(bucketName, path)
            if (om == null) return 0
            return om.getLastModified()?.getTime()
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                logger.warn("Not found (404) error in getLastModified for bucket ${bucketName} path ${path}: ${e.toString()}")
                return 0
            } else { throw e }
        }
    }

    @Override boolean supportsSize() { true }
    @Override long getSize() {
        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            ObjectMetadata om = s3Client.getObjectMetadata(bucketName, path)
            if (om == null) return 0
            // NOTE: or use getInstanceLength()?
            return om.getContentLength()
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                logger.warn("Not found (404) error in getSize for bucket ${bucketName} path ${path}: ${e.toString()}")
                return 0
            } else { throw e }
        }
    }

    @Override boolean supportsWrite() { true }
    @Override void putText(String text) {
        // FUTURE: use diff from last version for text
        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        if (autoCreateBucket && !s3Client.doesBucketExistV2(bucketName)) s3Client.createBucket(bucketName)

        s3Client.putObject(bucketName, path, text)
    }
    @Override void putStream(InputStream stream) {
        if (stream == null) return
        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        if (autoCreateBucket && !s3Client.doesBucketExistV2(bucketName)) s3Client.createBucket(bucketName)

        // NOTE: can specify more options using ObjectMetadata object as 4th parameter
        // NOTE: return PutObjectResult with more info, including version/etc
        // FUTURE: somehow ObjectMetadata.setContentLength()? without that will locally buffer entire stream to calculate length, ie Content-Length HTTP header required for REST API
        s3Client.putObject(bucketName, path, stream, null)
    }

    @Override void move(String newLocation) {
        if (!newLocation) throw new BaseArtifactException("No location specified, not moving resource at ${getLocation()}")
        if (!newLocation.startsWith(locationPrefix))
            throw new BaseArtifactException("Location [${newLocation}] is not a s3 location, not moving resource at ${getLocation()}")

        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        String newBucketName = getBucketName(newLocation)
        String newPath = getPath(newLocation)

        try {
            if (autoCreateBucket && bucketName != newBucketName && !s3Client.doesBucketExistV2(newBucketName)) s3Client.createBucket(newBucketName)

            // if this is a file move directly, if a directory move all files with its prefix
            if ((knownDirectory != null && !knownDirectory.booleanValue()) || s3Client.doesObjectExist(bucketName, path)) {
                // FUTURE: handle source version somehow, maybe different move or copy method? pass as third parameter to CopyObjectRequest constructor
                s3Client.copyObject(bucketName, path, newBucketName, newPath)
                s3Client.deleteObject(bucketName, path)
            } else {
                ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(path + "/")
                ListObjectsV2Result result = s3Client.listObjectsV2(lor)
                // objects (files in directory and all sub-directories)
                List<S3ObjectSummary> objectList = result.getObjectSummaries()
                for (S3ObjectSummary s3os in objectList) {
                    String srcPath = s3os.getKey()
                    String destPath = srcPath.replace(path, newPath)
                    s3Client.copyObject(bucketName, srcPath, newBucketName, destPath)
                    s3Client.deleteObject(bucketName, srcPath)
                }
            }
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
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
        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        if (!s3Client.doesObjectExist(bucketName, path)) return false
        s3Client.deleteObject(bucketName, path)
        return true
    }

    @Override boolean supportsVersion() { return true }
    @Override Version getVersion(String versionName) {
        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            GetObjectMetadataRequest gomr = new GetObjectMetadataRequest(bucketName, path)
            if (versionName) gomr.withVersionId(versionName)
            ObjectMetadata om = s3Client.getObjectMetadata(gomr)
            if (!om.getVersionId()) return null
            // TODO: use setUserMetadata(Map<String,String> userMetadata) and getUserMetadata() for userId, needs to be on app puts/etc
            // TODO: worth a separate request to try to get previousVersionName? doesn't seem to be easy way to do that either...
            return new Version(this, om.getVersionId(), null, null, new Timestamp(om.getLastModified().getTime()))
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                logger.warn("Not found (404) error in getVersion for bucket ${bucketName} path ${path}: ${e.toString()}")
                return null
            } else { throw e }
        }
    }
    @Override Version getCurrentVersion() { return getVersion(null) }
    @Override Version getRootVersion() {
        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            // NOTE: assuming this does oldest first, needs testing, docs not clear on any of this stuff
            ListVersionsRequest lvr = new ListVersionsRequest().withBucketName(bucketName).withPrefix(path).withMaxResults(1)
            VersionListing vl = s3Client.listVersions(lvr)
            List<S3VersionSummary> s3vsList = vl.getVersionSummaries()
            if (s3vsList == null || s3vsList.size() == 0) return null
            S3VersionSummary s3vs = s3vsList.get(0)
            return new Version(this, s3vs.getVersionId(), null, null, new Timestamp(s3vs.getLastModified().getTime()))
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                logger.warn("Not found (404) error in getRootVersion for bucket ${bucketName} path ${path}: ${e.toString()}")
                return null
            } else { throw e }
        }
    }
    @Override ArrayList<Version> getVersionHistory() {
        return getNextVersions(null)
    }
    @Override ArrayList<Version> getNextVersions(String versionName) {
        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        ListVersionsRequest lvr = new ListVersionsRequest().withBucketName(bucketName).withPrefix(path)
        // NOTE: any way to get versions that have this versionName as the previous version? doesn't seem so, ie no branching just linear list so just get next version
        if (versionName != null && !versionName.isEmpty()) lvr.withVersionIdMarker(versionName).withMaxResults(1)

        try {
            VersionListing vl = s3Client.listVersions(lvr)
            List<S3VersionSummary> s3vsList = vl.getVersionSummaries()
            ArrayList<Version> verList = new ArrayList<>(s3vsList.size())
            String prevVersion = null
            for (S3VersionSummary s3vs in s3vsList) {
                verList.add(new Version(this, s3vs.getVersionId(), prevVersion, null, new Timestamp(s3vs.getLastModified().getTime())))
                prevVersion = s3vs.getVersionId()
            }
            return verList
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                logger.warn("Not found (404) error in getNextVersions for bucket ${bucketName} path ${path} version ${versionName}: ${e.toString()}")
                return null
            } else { throw e }
        }
    }
    @Override InputStream openStream(String versionName) {
        if (versionName == null || versionName.isEmpty()) return openStream()

        AmazonS3 s3Client = getS3Client()
        String bucketName = getBucketName(location)
        String path = getPath(location)

        try {
            GetObjectRequest gor = new GetObjectRequest(bucketName, path, versionName)
            S3Object obj = s3Client.getObject(gor)
            S3ObjectInputStream s3is = obj.getObjectContent()
            return s3is
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                logger.warn("Not found (404) error in openStream for bucket ${bucketName} path ${path} version ${versionName}: ${e.toString()}")
                return null
            } else { throw e }
        }
    }
    @Override String getText(String versionName) { return ObjectUtilities.getStreamText(openStream(versionName)) }

    AmazonS3 getS3Client() {
        AmazonS3 s3Client = ecf.getTool(S3ClientToolFactory.TOOL_NAME, AmazonS3.class)
        if (s3Client == null) throw new BaseException("AWS S3 Client not initialized")
        return s3Client
    }
}
