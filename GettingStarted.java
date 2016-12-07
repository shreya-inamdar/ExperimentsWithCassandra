import com.datastax.driver.core.Cluster;  
import com.datastax.driver.core.Host;  
import com.datastax.driver.core.Metadata;  
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;  
import com.datastax.driver.core.Row;

public class GettingStarted{
    public static void main(String [] args){
	Cluster cluster = null;
	try{
	    cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	    Session session = cluster.connect();
	    System.out.println("Entered at least.");
	    ResultSet rs = session.execute("select release_version from system.local");
	    Row row = rs.one();
	    System.out.println(row.getString("release_version"));
	}
	finally{
	    if (cluster != null)
		cluster.close();
	}
    }
}
