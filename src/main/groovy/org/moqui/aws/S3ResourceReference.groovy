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

import org.moqui.BaseArtifactException
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.context.reference.BaseResourceReference
import org.moqui.resource.ResourceReference
import org.moqui.util.ObjectUtilities
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.rowset.serial.SerialBlob
import java.nio.charset.StandardCharsets

class S3ResourceReference extends BaseResourceReference {
    protected final static Logger logger = LoggerFactory.getLogger(S3ResourceReference.class)
    public final static String locationPrefix = "s3://"

    String location

    S3ResourceReference() { }

    @Override ResourceReference init(String location, ExecutionContextFactoryImpl ecf) {
        this.ecf = ecf
        this.location = location
        return this
    }

    @Override ResourceReference createNew(String location) {
        S3ResourceReference resRef = new S3ResourceReference()
        resRef.init(location, ecf)
        return resRef
    }
    @Override String getLocation() { location }

    String getBucketName() {
        if (!location) return ""
        // should have a prefix of "s3://" and then first path segment is bucket name
        String fullPath = location.substring(locationPrefix.length())
        int slashIdx = fullPath.indexOf("/")
        if (slashIdx) {
            return fullPath.substring(0, slashIdx)
        } else {
            return fullPath
        }
    }
    String getPath() {
        if (!location) return ""
        // should have a prefix of "s3://" and then first path segment is bucket name
        String fullPath = location.substring(locationPrefix.length())
        int slashIdx = fullPath.indexOf("/")
        if (slashIdx) {
            return fullPath.substring(slashIdx + 1)
        } else {
            return ""
        }
    }

    @Override InputStream openStream() {
        // TODO
        return null
    }

    @Override OutputStream getOutputStream() {
        // TODO can support this?
        throw new UnsupportedOperationException("The getOutputStream method is not supported for s3 resources, use putStream() instead")
    }

    @Override String getText() { return ObjectUtilities.getStreamText(openStream()) }

    @Override boolean supportsAll() { true }

    @Override boolean supportsUrl() { false }
    @Override URL getUrl() { return null }

    @Override boolean supportsDirectory() { true }
    @Override boolean isFile() {
        // TODO
        return true
    }
    @Override boolean isDirectory() {
        if (!getPath()) return true // consider root a directory
        // TODO
        return false
    }
    @Override List<ResourceReference> getDirectoryEntries() {
        // TODO
        return null
    }

    @Override boolean supportsExists() { true }
    @Override boolean getExists() { return getDbResource(true) != null }

    @Override boolean supportsLastModified() { true }
    @Override long getLastModified() {
        // TODO
        return 0
    }

    @Override boolean supportsSize() { true }
    @Override long getSize() {
        // TODO
        return 0
    }

    @Override boolean supportsWrite() { true }
    @Override void putText(String text) {
        // TODO: use diff from last version for text
        SerialBlob sblob = text ? new SerialBlob(text.getBytes(StandardCharsets.UTF_8)) : null
        // TODO
    }
    @Override void putStream(InputStream stream) {
        if (stream == null) return
        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        ObjectUtilities.copyStream(stream, baos)
        SerialBlob sblob = new SerialBlob(baos.toByteArray())
        // TODO
    }

    @Override void move(String newLocation) {
        if (!newLocation) throw new BaseArtifactException("No location specified, not moving resource at ${getLocation()}")
        if (!newLocation.startsWith(locationPrefix))
            throw new BaseArtifactException("Location [${newLocation}] is not a s3 location, not moving resource at ${getLocation()}")

        List<String> filenameList = new ArrayList<>(Arrays.asList(newLocation.substring(locationPrefix.length()).split("/")))
        if (filenameList) {
            String newFilename = filenameList.get(filenameList.size()-1)
            filenameList.remove(filenameList.size()-1)
            // TODO
        }
    }

    @Override ResourceReference makeDirectory(String name) {
        // TODO can make directory with no files in S3?
        return new S3ResourceReference().init("${location}/${name}", ecf)
    }
    @Override ResourceReference makeFile(String name) {
        S3ResourceReference newRef = new S3ResourceReference().init("${location}/${name}", ecf)
        // TODO make empty file
        return newRef
    }
    @Override boolean delete() {
        // TODO
        // if not exists: return false
        return true
    }

    @Override boolean supportsVersion() { return true }
    @Override Version getVersion(String versionName) {
        // TODO
        return null
    }
    @Override Version getCurrentVersion() {
        // TODO
        return null
    }
    @Override Version getRootVersion() {
        // TODO
        return null
    }
    @Override ArrayList<Version> getVersionHistory() {
        // TODO
        ArrayList<Version> verList = new ArrayList<>()
        return verList
    }
    @Override ArrayList<Version> getNextVersions(String versionName) {
        // TODO
        ArrayList<Version> verList = new ArrayList<>()
        return verList
    }
    @Override InputStream openStream(String versionName) {
        if (versionName == null || versionName.isEmpty()) return openStream()
        // TODO
        return null
    }
    @Override String getText(String versionName) { return ObjectUtilities.getStreamText(openStream(versionName)) }
}
