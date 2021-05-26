
import java.util.{Properties}
import java.net._
import java.io._
import org.json._
import java.nio.charset.StandardCharsets;


import scala.collection.immutable._

object Utils {
  
  
  def getTwitterProperties() : Properties = 
  {
    
    val reader=new FileReader("/home/hdpadmin/workspace/mytwitterApp/twConf.properties");  
      
    val  p =new Properties();  
    p.load(reader);  
    
    p
    
  }
  
  def post_to_flaskDashboard(url:String , label:List[String] ,  data:List[String]) :Unit = 
  {
   

    val json = new JSONObject(); 
    json.put("hashTags", label.mkString("[", ",", "]"))
    json.put("count" , data.mkString("[", ",", "]"))
   
    
     val obj = new URL(url)
     
     val con =  obj.openConnection().asInstanceOf[HttpURLConnection]
     con.setRequestMethod("POST")
     con.setRequestProperty("Content-Type", "application/json");
     con.setRequestProperty("User-Agent", "Mozilla/5.0");
     con.setRequestProperty("Accept", "application/json; utf-8");
     //con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
     
     con.setDoOutput(true);
     val wr = new DataOutputStream(con.getOutputStream());
    
     wr.write(json.toString().getBytes);
     
     wr.flush()
      
     val responseCode = con.getResponseCode();
     
     println(s" responseCode : $responseCode")
     //println(json)
    

    
  } 

}
