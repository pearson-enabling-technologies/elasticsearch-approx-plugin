package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TermListFacetTest {

    private static Node __node;
    @BeforeClass
    public static void setUpClass() {
        
        
        
        System.out.println("test0");
    }

    @AfterClass
    public static void tearDownClass() {
        System.out.println("test1");
    }

    @Before
    public void setUp() throws IOException {
        System.out.println("test21");
    }

    @Test
    public void testWithMaxOneDocPerDayBucketOnAtomicField() throws Exception {
        System.out.println("test2");
        
        
        
        
        try{
            System.out.println("test00");
            final Settings settings = ImmutableSettings.settingsBuilder()
                    
                    .put("node.http.enabled", false)
                    .put("index.gateway.type", "none")
                    // Reluctantly removed this to reduce overall memory:
                    //.put("index.store.type", "memory")
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 0)
                    .put("path.data", "target")
                    .put("refresh_interval", -1)
                    .put("index.cache.field.type", "soft")
                    .build();
            
            
            __node = nodeBuilder()
                    .local(true)
                    .settings(settings)
                    .clusterName("DistinctDateHistogramFacetTest")
                    .node();
            }
            catch(final Exception ex){
                
                System.out.println(ex.getMessage());
            }
            
    }

}
