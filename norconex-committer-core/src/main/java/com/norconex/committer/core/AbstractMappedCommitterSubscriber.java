/* Copyright 2010-2017 Norconex Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.norconex.committer.core;

import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.config.XMLConfigurationUtil;
import com.norconex.commons.lang.time.DurationParser;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

/**
 * A base class batching documents and offering mappings of source reference and source content
 * fields to target reference and target content fields. Batched documents are placed on a reactive
 * queued.
 *
 * <h3>Reference Mapping:</h3>
 *
 * <h4>Source document reference</h4>
 *
 * <p>By default the document reference from the source document comes from document reference value
 * passed to the Committer (obtained internally using {@link IAddOperation#getReference()} or {@link
 * IDeleteOperation#getReference()}). If you wish to ignore that original document reference and use
 * a metadata field instead, use the {@link #setSourceReferenceField(String)} method to do so.
 *
 * <h4>Target document reference</h4>
 *
 * <p>The default (or constant) target reference field is for subclasses to define.
 *
 * <p>When both a source and target reference fields are defined, the source reference field will be
 * deleted unless the <code>keepSourceReferenceField</code> attribute is set to <code>true</code>.
 *
 * <h3>Content Mapping:</h3>
 *
 * <p>Content typically only occurs when committing additions.
 *
 * <h4>Source document content</h4>
 *
 * <p>The default source document content is the actual document content (obtained internally using
 * {@link IAddOperation#getContentStream()}). Defining a <code>sourceContentField</code> will use
 * the matching metadata property instead.
 *
 * <h4>Target document content</h4>
 *
 * <p>The default (or constant) <b>target content</b> field is for subclasses to define.
 *
 * <p>When both a source and target content fields are defined, the source content field will be
 * deleted unless the <code>keepSourceContentField</code> attribute is set to <code>true</code>.
 *
 * <p>As of 2.1.0, XML configuration entries expecting millisecond durations can be provided in
 * human-readable format (English only), as per {@link DurationParser} (e.g., "5 minutes and 30
 * seconds" or "5m30s"). <a id="xml-config"></a>
 *
 * <h3>XML Configuration</h3>
 *
 * <p>Subclasses implementing {@link IXMLConfigurable} should allow this inner configuration:
 *
 * <pre>
 *      &lt;sourceReferenceField keep="[false|true]"&gt;
 *         (Optional name of field that contains the document reference, when
 *         the default document reference is not used.  The reference value
 *         will be mapped to the "targetReferenceField"
 *         specified or target repository default field if one is defined
 *         by the concrete implementation.
 *         Once re-mapped, this metadata source field is
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceReferenceField&gt;
 *      &lt;targetReferenceField&gt;
 *         (Name of target repository field where to store a document reference.
 *         If not specified, behavior is defined
 *         by the concrete implementation.)
 *      &lt;/targetReferenceField&gt;
 *      &lt;sourceContentField keep="[false|true]"&gt;
 *         (If you wish to use a metadata field to act as the document
 *         "content", you can specify that field here.  Default
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceContentField&gt;
 *      &lt;targetContentField&gt;
 *         (Target repository field name for a document content/body.
 *          Default is defined by concrete implementation.)
 *      &lt;/targetContentField&gt;
 *      &lt;commitBatchSize&gt;
 *          (max number of documents to send to target repository at once)
 *      &lt;/commitBatchSize&gt;
 *      &lt;maxRetries&gt;(max retries upon commit failures)&lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;(max delay in milliseconds between retries)&lt;/maxRetryWait&gt;
 * </pre>
 *
 * @author Jeroen Steggink
 * @since 2.1.3
 */
public abstract class AbstractMappedCommitterSubscriber extends AbstractBatchCommitterSubscriber {

    // Source fields
    private String sourceReferenceField;
    private String sourceContentField;
    private boolean keepSourceReferenceField;
    private boolean keepSourceContentField;

    // Target fields
    private String targetReferenceField;
    private String targetContentField;

    /** Creates a new instance. */
    public AbstractMappedCommitterSubscriber() {

    }

    /**
     * Gets the source field name holding the unique identifier.
     *
     * @return source field name
     */
    public String getSourceReferenceField() {
        return sourceReferenceField;
    }

    /**
     * sets the source field name holding the unique identifier.
     *
     * @param sourceReferenceField source field name
     */
    public void setSourceReferenceField(String sourceReferenceField) {
        this.sourceReferenceField = sourceReferenceField;
    }

    /**
     * Gets the target field name to store the unique identifier.
     *
     * @return target field name
     */
    public String getTargetReferenceField() {
        return targetReferenceField;
    }

    /**
     * Sets the target field name to store the unique identifier.
     *
     * @param targetReferenceField target field name
     */
    public void setTargetReferenceField(String targetReferenceField) {
        this.targetReferenceField = targetReferenceField;
    }

    /**
     * Gets the target field where to store the document content.
     *
     * @return target field name
     */
    public String getTargetContentField() {
        return targetContentField;
    }

    /**
     * Sets the target field where to store the document content.
     *
     * @param targetContentField target field name
     */
    public void setTargetContentField(String targetContentField) {
        this.targetContentField = targetContentField;
    }

    /**
     * Gets the source field name holding the document content.
     *
     * @return source field name
     */
    public String getSourceContentField() {
        return sourceContentField;
    }

    /**
     * Sets the source field name holding the document content.
     *
     * @param sourceContentField source field name
     */
    public void setSourceContentField(String sourceContentField) {
        this.sourceContentField = sourceContentField;
    }

    /**
     * Whether to keep the reference source field or not, once mapped.
     *
     * @return <code>true</code> when keeping source reference field
     */
    public boolean isKeepSourceReferenceField() {
        return keepSourceReferenceField;
    }

    /**
     * Sets whether to keep the ID source field or not, once mapped.
     *
     * @param keepSourceReferenceField <code>true</code> when keeping source reference field
     */
    public void setKeepSourceReferenceField(boolean keepSourceReferenceField) {
        this.keepSourceReferenceField = keepSourceReferenceField;
    }

    /**
     * Whether to keep the content source field or not, once mapped.
     *
     * @return <code>true</code> when keeping content source field
     */
    public boolean isKeepSourceContentField() {
        return keepSourceContentField;
    }

    /**
     * Sets whether to keep the content source field or not, once mapped.
     *
     * @param keepSourceContentField <code>true</code> when keeping content source field
     */
    public void setKeepSourceContentField(boolean keepSourceContentField) {
        this.keepSourceContentField = keepSourceContentField;
    }

    public void saveToXML(XMLStreamWriter out) throws XMLStreamException {
        EnhancedXMLStreamWriter writer = new EnhancedXMLStreamWriter(out);

            if (sourceReferenceField != null) {
                writer.writeStartElement("sourceReferenceField");
                writer.writeAttributeBoolean("keep", keepSourceReferenceField);
                writer.writeCharacters(sourceReferenceField);
                writer.writeEndElement();
            }

            if (targetReferenceField != null) {
                writer.writeElementString("targetReferenceField", targetReferenceField);
            }
            if (sourceContentField != null) {
                writer.writeStartElement("sourceContentField");
                writer.writeAttributeBoolean("keep", keepSourceContentField);
                writer.writeCharacters(sourceContentField);
                writer.writeEndElement();
            }
            if (targetContentField != null) {
                writer.writeElementString("targetContentField", targetContentField);
            }
    }

    public void loadFromXML(XMLConfiguration xml) {
        setSourceReferenceField(xml.getString("sourceReferenceField", sourceReferenceField));
        setKeepSourceReferenceField(
                xml.getBoolean("sourceReferenceField[@keep]", keepSourceReferenceField));
        setTargetReferenceField(xml.getString("targetReferenceField", targetReferenceField));
        setSourceContentField(xml.getString("sourceContentField", sourceContentField));
        setKeepSourceContentField(xml.getBoolean("sourceContentField[@keep]", keepSourceContentField));
        setTargetContentField(xml.getString("targetContentField", targetContentField));
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .appendSuper(super.hashCode())
                .append(sourceContentField)
                .append(keepSourceContentField)
                .append(targetContentField)
                .append(sourceReferenceField)
                .append(keepSourceReferenceField)
                .append(targetReferenceField)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof AbstractMappedCommitterSubscriber)) {
            return false;
        }
        AbstractMappedCommitterSubscriber other = (AbstractMappedCommitterSubscriber) obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(sourceContentField, other.sourceContentField)
                .append(keepSourceContentField, other.keepSourceContentField)
                .append(targetContentField, other.targetContentField)
                .append(sourceReferenceField, other.sourceReferenceField)
                .append(keepSourceReferenceField, other.keepSourceReferenceField)
                .append(targetReferenceField, other.targetReferenceField)
                .isEquals();
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        builder.appendSuper(super.toString());
        builder.append("targetReferenceField", targetReferenceField);
        builder.append("sourceReferenceField", sourceReferenceField);
        builder.append("keepSourceReferenceField", keepSourceReferenceField);
        builder.append("targetContentField", targetContentField);
        builder.append("sourceContentField", sourceContentField);
        builder.append("keepSourceContentField", keepSourceContentField);
        return builder.toString();
    }
}
