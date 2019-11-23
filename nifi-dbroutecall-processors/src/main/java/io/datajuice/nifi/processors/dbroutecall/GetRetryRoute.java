/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datajuice.nifi.processors.dbroutecall;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.datajuice.nifi.processors.dbroutecall.Properties.*;
import static io.datajuice.nifi.processors.dbroutecall.Relationships.REL_FAILURE;
import static io.datajuice.nifi.processors.dbroutecall.Relationships.REL_SUCCESS;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GetRetryRoute extends AbstractProcessor {

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    protected DBCPService dbcpService;


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(DBCP_SERVICE);
        descriptors.add(PROCESS_GROUP);
        descriptors.add(PROCESSOR);
        descriptors.add(ATTEMPT_NUM);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final String processor = context.getProperty(PROCESSOR).evaluateAttributeExpressions(flowFile).getValue();
        final String processGroup = context.getProperty(PROCESS_GROUP).evaluateAttributeExpressions(flowFile).getValue();
        final String attempt = context.getProperty(ATTEMPT_NUM).evaluateAttributeExpressions(flowFile).getValue();

        final String preSelectQuery =
                "select a.route " +
                "FROM error_retry_framework a " +
                "JOIN " +
                "   (SELECT " +
                "       route," +
                "       max(attempt) max_attempt " +
                "   FROM error_retry_framework " +
                "   where process_group = ''{0}'' " +
                "   and processor = ''{1}'' " +
                "   and attempt <= {2} " +
                "   GROUP BY route) x " +
                "ON a.route = x.route " +
                "AND a.attempt = x.max_attempt";
        final String selectQuery = MessageFormat.format(preSelectQuery, processGroup, processor, attempt);


        try (final Connection con = dbcpService.getConnection();
             final PreparedStatement st = con.prepareStatement(selectQuery)) {
            boolean hasResults = st.execute();

            if (hasResults) {
                final ResultSet resultSet = st.getResultSet();
                resultSet.next();
                String route = resultSet.getString("route");

                session.putAttribute(flowFile, "failureRoute", route);

            } else throw new ProcessException("Result set empty");


            session.transfer(flowFile, REL_SUCCESS);
        } catch (SQLException | ProcessException e) {
            session.putAttribute(flowFile, "error", e.toString());
            session.transfer(flowFile, REL_FAILURE);
        }

    }
}
