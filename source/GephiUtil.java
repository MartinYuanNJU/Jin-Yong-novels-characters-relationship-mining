/*
import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.viz.NodeShape;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Calendar;


public class GephiUtil {

	public static void main(String[] args) {
		Gexf gexf = new GexfImpl();
		Calendar date = Calendar.getInstance();
		
		gexf.getMetadata()
			.setLastModified(date.getTime())
			.setCreator("Gephi.org")
			.setDescription("A Web network");
		gexf.setVisualization(true);

		Graph graph = gexf.getGraph();
		graph.setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.STATIC);
		
		AttributeList attrList = new AttributeListImpl(AttributeClass.NODE);
		graph.getAttributeLists().add(attrList);
		
		Attribute attUrl = attrList.createAttribute("0", AttributeType.STRING, "url");
		Attribute attIndegree = attrList.createAttribute("1", AttributeType.FLOAT, "indegree");
		Attribute attFrog = attrList.createAttribute("2", AttributeType.BOOLEAN, "frog")
			.setDefaultValue("true");
	 
		
		Node gephi = graph.createNode("0");
		gephi
			.setLabel("Gephi")
			.setSize(20)
			.getAttributeValues()
				.addValue(attUrl, "http://gephi.org")
				.addValue(attIndegree, "1");
		gephi.getShapeEntity().setNodeShape(NodeShape.DIAMOND).setUri("GephiURI");
		
		Node webatlas = graph.createNode("1");
		webatlas
			.setLabel("Webatlas")
			.getAttributeValues()
				.addValue(attUrl, "http://webatlas.fr")
				.addValue(attIndegree, "2");
		
		Node rtgi = graph.createNode("2");
		rtgi
			.setLabel("RTGI")
			.getAttributeValues()
				.addValue(attUrl, "http://rtgi.fr")
				.addValue(attIndegree, "1");
		
		Node blab = graph.createNode("3");
		blab
			.setLabel("BarabasiLab")
			.getAttributeValues()
				.addValue(attUrl, "http://barabasilab.com")
				.addValue(attIndegree, "1")
				.addValue(attFrog, "false");
		
		gephi.connectTo("0", webatlas);
		gephi.connectTo("1", rtgi);
		webatlas.connectTo("2", gephi);
		rtgi.connectTo("3", webatlas);
		gephi.connectTo("4", blab);

		StaxGraphWriter graphWriter = new StaxGraphWriter();
		File f = new File("static_graph_sample.gexf");
		Writer out;
		try {
			out =  new FileWriter(f, false);
			graphWriter.writeToStream(gexf, out, "UTF-8");
			System.out.println(f.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}*/

import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class GephiUtil {
    public static void main(String[] args) throws IOException {
    	String root = System.getProperty("user.dir")+File.separator+"src"+File.separator+"main"+File.separator+"resources"+File.separator;
    	String fileName1="task5_results.txt";
    	String fileName2="task3_results.txt";
    	String filePath1 = root+fileName1;
    	String filePath2 = root+fileName2;
        BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(filePath1),"UTF-8"));
        BufferedReader br2 = new BufferedReader(new InputStreamReader(new FileInputStream(filePath2),"UTF-8"));
        Gexf gexf = new GexfImpl();
        Calendar date = Calendar.getInstance();

        gexf.getMetadata()
                .setLastModified(date.getTime())
                .setCreator("171860665_xh")
                .setDescription("A visualization of task5");
        gexf.setVisualization(true);

        Graph graph = gexf.getGraph();
        graph.setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.STATIC);

        AttributeList attrList = new AttributeListImpl(AttributeClass.NODE);
        graph.getAttributeLists().add(attrList);

        Attribute attLabel = attrList.createAttribute("class", AttributeType.INTEGER, "Class");
        Attribute attIndegree = attrList.createAttribute("pageranks", AttributeType.DOUBLE, "PageRank");

        String line = "";
        int index = 0;
        Map<String,Node> node_map = new HashMap<>();
        while((line = br1.readLine())!= null){
            String[] lpaList = line.split("\t");
            String name = lpaList[0];
            String label = lpaList[1];
            String pageRank = lpaList[2];
            Node node = node_map.get(name);
            if (node == null){
                index++;
                node = graph.createNode(String.valueOf(index));
                node.setLabel(name)
                        .getAttributeValues()
                        .addValue(attLabel, label)
                        .addValue(attIndegree, pageRank);
                node_map.put(name,node);
            }
            else{
                node.getAttributeValues().addValue(attIndegree,pageRank);
            }
        }
        br1.close();
        
        line = "";
        int edge_index = 0;
        while((line = br2.readLine())!=null) {
        	int index_t = line.indexOf('\t');
        	String name = line.substring(0, index_t);
        	String nameList = line.substring(index_t+1);
        	Node node = node_map.get(name);
            for (String namePair:nameList.split(" ")){
                String[] name_value_pair = namePair.split(":");
                Node name_node = node_map.get(name_value_pair[0]);
                edge_index++;
                node.connectTo(String.valueOf(edge_index), name_node).setWeight(Float.parseFloat(name_value_pair[1]));
            }
        }
        br2.close();
        
        StaxGraphWriter graphWriter = new StaxGraphWriter();
        String outPath = root+"task6_results.gexf";
        File f = new File(outPath);
        Writer out;
        try {
            out =  new FileWriter(f, false);
            graphWriter.writeToStream(gexf, out, "UTF-8");
            System.out.println(f.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
     }
   
}