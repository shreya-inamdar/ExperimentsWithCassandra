import com.datastax.driver.core.Cluster;  
import com.datastax.driver.core.Host;  
import com.datastax.driver.core.Metadata;  
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;  
import com.datastax.driver.core.Row;
import java.util.concurrent.TimeUnit;

public class Counter{
    private static String nodeId;
    private static Session session;
    private static Cluster cluster;
    public static void main(String [] args){
	nodeId = args[0];
	System.out.println(nodeId);
	cluster = null;

	try{
     	    StringBuilder cp = new StringBuilder("127.0.0.");
	    cp = cp.append(nodeId);
	    cluster = Cluster.builder().addContactPoint(cp.toString()).build();
	    Session session = cluster.connect();
	    /*
	    System.out.println("Trial commands...");
	    ResultSet rs = session.execute("select release_version from system.local");
	    Row row = rs.one();
	    System.out.println(row.getString("release_version"));
	    System.out.println("Trial done.");
	    */
	    
	    for(int i =0; i < 10; i++){
		try{
		    TimeUnit.SECONDS.sleep(1);
		}
		catch (Exception e){
		    System.out.println("Exception in TimeUnit");
		}
		if(i%2 == 0){
		    inc();
		}
		else{
		    value();
		}
	    }
	}
	finally{
	    if (cluster != null)
		cluster.close();
	}
    }

    public static long value(){
	String query = "SELECT COUNT(*) FROM ctr.counter;";
	ResultSet rs = session.execute(query);
	Row row = rs.one();
	long res = row.getLong("COUNT");
	System.out.println(res);
	return res;
    }

    public static void inc(){
	long ts = System.currentTimeMillis()/1000;
	StringBuilder sb = new StringBuilder("INSERT INTO ctr.counter (localId, value, nodeid) values ( ");
	sb.append(ts);
	sb.append(", 1, ");
	sb.append(nodeId);
	sb.append(")");
	String query = sb.toString();
	session = cluster.connect();	    
	ResultSet rs = session.execute(query);
	return;
    }
}

//create table counter ( localId timestamp, value int, nodeid int, primary key (localId, nodeid) ) ;
//insert into ctr.counter (localId, value, nodeid) values (1234557, 1, 1);
//select count(*) from ctr.counter;
//javac -classpath cassandra-java-driver-3.0.5/cassandra-driver-core-3.0.5.jar:. Counter.java
//java -classpath cassandra-java-driver-3.0.5/*:cassandra-java-driver-3.0.5/lib/*:. Counter 2
