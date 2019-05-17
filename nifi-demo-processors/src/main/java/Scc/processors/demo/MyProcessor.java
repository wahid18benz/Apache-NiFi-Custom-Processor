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
package Scc.processors.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;



@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {


    public static final PropertyDescriptor IGNITE_HOST = new PropertyDescriptor
            .Builder().name("IGNITE HOST")
            .displayName("IGNITE HOST")
            .description("Ignite URI Or Ip Address")
            .defaultValue("127.0.0.1")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor IGNITE_PORT = new PropertyDescriptor
            .Builder().name("IGNITE PORT")
            .displayName("IGNITE PORT")
            .description("Ignite Port")
            .defaultValue("10800")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


 /*   public static final PropertyDescriptor IGNITE_CACHE_NAME = new PropertyDescriptor
            .Builder().name("CACHE NAME")
            .displayName("CACHE NAME")
            .description("Ignite Cache Name to which to Connect")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IGNITE_KEY= new PropertyDescriptor
            .Builder().name("IGNITE KEY")
            .displayName("IGNITE KEY")
            .description("Key ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor IGNITE_VALUE = new PropertyDescriptor
            .Builder().name("IGNITE VALUE")
            .displayName("IGNITE VALUE")
            .description("Value")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
*/

    public static final PropertyDescriptor IGNITE_TABLE_NAME = new PropertyDescriptor
            .Builder().name("IGNITE TABLE NAME")
            .displayName("IGNITE TABLE NAME")
            .description("Ignite Table Name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IGNITE_TABLE_COLUMNS = new PropertyDescriptor
            .Builder().name("IGNITE TABLE COLUMNS")
            .displayName("IGNITE TABLE COLUMNS")
            .description("IGNITE TABLE COLUMNS Formate :{columns:[{name:,type:},...],PRIMARY KEY:[column_name1,....]} ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IGNITE_QUERY_COLUMNS = new PropertyDescriptor
            .Builder().name("IGNITE QUERY COLUMNS")
            .displayName("IGNITE QUERY COLUMNS")
            .description("IGNITE QUERY COLUMNS  Formate : {columns:[column_name1,...]}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IGNITE_QUERY_VALUES = new PropertyDescriptor
            .Builder().name("IGNITE QUERY VALUES")
            .displayName("IGNITE QUERY VALUES")
            .description("IGNITE QUERY VALUES Format :{values:[value1,value2,...]}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)            
            .build();


    
    
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Success relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failure relationship")
            .build();
    
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(IGNITE_HOST);
        descriptors.add(IGNITE_PORT);
        descriptors.add(IGNITE_TABLE_NAME);
        descriptors.add(IGNITE_TABLE_COLUMNS);
        descriptors.add(IGNITE_QUERY_COLUMNS);
        descriptors.add(IGNITE_QUERY_VALUES);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
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
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        
        final String IgniteHost = context.getProperty(IGNITE_HOST).getValue();
        final String IgnitePort = context.getProperty(IGNITE_PORT).getValue();
  
        /*final String HOST = "127.0.0.1";
		final String PORT = "10800";*/
		final String TABLE_NAME = context.getProperty(IGNITE_TABLE_NAME).getValue();
		final String TABLE_COLUMNS = context.getProperty(IGNITE_TABLE_COLUMNS).getValue();
		final String QUERY_COLUMNS = context.getProperty(IGNITE_QUERY_COLUMNS).getValue();
		final String QUERY_VALUES =  context.getProperty(IGNITE_QUERY_VALUES).evaluateAttributeExpressions(flowFile).getValue(); // add evaluate expression 
	 
	    HashMap<String, Object> yourHashMap = new Gson().fromJson(TABLE_COLUMNS.toString(), HashMap.class);
	    HashMap<String, Object> queryHashMap = new Gson().fromJson(QUERY_COLUMNS.toString(), HashMap.class);
	    HashMap<String, Object> valuesHashMap = new Gson().fromJson(QUERY_VALUES.toString(), HashMap.class);
       
        ClientConfiguration cfg = new ClientConfiguration().setAddresses(IgniteHost + ":" + IgnitePort);
        
        try (IgniteClient igniteClient = Ignition.startClient(cfg)) {
        	
        	ArrayList<?> Cols = (ArrayList<?>) yourHashMap.get("columns");
		    String req ="CREATE TABLE IF NOT EXISTS "+ TABLE_NAME +"(" ;
		     
		    Iterator<?> iterCols = Cols.iterator();
		    while(iterCols.hasNext()) {
		    	LinkedTreeMap<?, ?> col = (LinkedTreeMap<?, ?>) iterCols.next();
		    	req =req.concat(((String) (col.get("name")))
		    			.concat(" ")
		    			.concat(((String) (col.get("type"))))
		    			.concat(", "));
		    }	

		    req = req.concat(" PRIMARY KEY(");
	        
	        ArrayList<?> pKeys = (ArrayList<?>) yourHashMap.get("PRIMARY KEY");
	        Iterator<?> iterKeys = pKeys.iterator();
			         
		    while(iterKeys.hasNext()) {	 
		    	req =req.concat(((String) (iterKeys.next()))
		    			.concat(", "));
			 }
	 
	        req = req.substring(0,req.length()-2);
	        req =req.concat("))");
	        	
	        getLogger().info("Simple Ignite thin client example working over TCP socket.");
			igniteClient.query(
				new SqlFieldsQuery(String.format(req))
				.setSchema("PUBLIC"))
			.getAll();

			ArrayList<?> QCols = (ArrayList<?>) queryHashMap.get("columns");		         
			req ="MERGE INTO "+ TABLE_NAME +" (" ;
			Iterator<?> iterColsQ = QCols.iterator();
         
	        while(iterColsQ.hasNext()) {
	        	req =req.concat(((String) (iterColsQ.next()))
	        			.concat(", "));
	        }
         	  
         	req = req.substring(0,req.length()-2);
         	req = req.concat(")VALUES(");
         
         	ArrayList<?> VCols = (ArrayList<?>) valuesHashMap.get("values");
	        Iterator<?> iterColsV = VCols.iterator();
			         
			while(iterColsV.hasNext()) {
			    req =req.concat(((String) (iterColsV.next()))
			    		.concat(", "));
			}
	 
	        req = req.substring(0,req.length()-2);
	        req = req.concat(")");

			igniteClient.query(new SqlFieldsQuery(req)
						.setSchema("PUBLIC")).getAll();
		  
        	session.transfer(flowFile, REL_SUCCESS);
	    }catch (IgniteException e) {
	    	getLogger().error("Ignite exception: {}", e);
	    	session.transfer(flowFile, REL_FAILURE);
	    } catch (Exception e) {
	    	getLogger().error("Ignite exception: {}", e);
	    	session.transfer(flowFile, REL_FAILURE);
	    }


		

    }
}
    
