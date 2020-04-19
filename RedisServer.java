import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Timer;
public class RedisServer
{
	private ServerSocket server;
	private Socket socket;
	private DataInputStream in;
	private DataOutputStream out;
	Map map;
	public RedisServer(int port)
	{
        try
        { 
            server = new ServerSocket(port);
		    System.out.println("Server started"); 
  			while(true)
  			{	
	            socket = server.accept(); 
	            in = new DataInputStream(socket.getInputStream());
	            out =new DataOutputStream(socket.getOutputStream());
	            File tempFile = new File("map.ser");
	            boolean exist=tempFile.exists();
	            tempFile.createNewFile();
	            FileInputStream fis = new FileInputStream("map.ser");
	            if(exist==false)
	            {
	            	map = new HashMap<String,String>();

	            }
	            else
	            {
	            	try{
	            	ObjectInputStream ois = new ObjectInputStream(fis);
       				map = (Map) ois.readObject();
       				ois.close();
       				}
       				catch(Exception ex)
       				{
       					System.out.println("phat gya");
       				}
	            }
	            ClientHandler mtch = new ClientHandler(socket,"client ", in, out,map); 
           		Thread t = new Thread(mtch);
           		t.start();
	           
	        }
	     } 
	    catch(IOException i) 
	        { 
	            System.out.println(i); 
	        }
	 }
        
	public static void main(String[] args) {
		
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter port on which server start running. Should be greater than 1023");
		try{
			int port=sc.nextInt();
			RedisServer redisServer = new RedisServer(port);	
		}
		catch(Exception ex)
		{
			System.out.println("Please enter numeric value");
		}
		
	}
}
class ClientHandler implements Runnable  
{ 
    private String name; 
    final DataInputStream dis; 
    final DataOutputStream dos; 
    Map map;
    Socket s; 
    boolean isloggedin; 
      
    public ClientHandler(Socket s, String name, 
                            DataInputStream dis, DataOutputStream dos,Map map) { 
        this.dis = dis; 
        this.dos = dos; 
        this.name = name; 
        this.s = s; 
        this.isloggedin=true; 
        this.map=map;
    } 
  
    @Override
    public void run() { 

  
        String received; 
        while (true)  
        { 
            try
            { 
                received = dis.readUTF(); 
                System.out.println(received); 
                if(received.equalsIgnoreCase("exit")){ 
                	dos.writeUTF("Thanks"); 
                    this.isloggedin=false; 
                    this.s.close(); 
                    break; 
                }
                else
                {
                	// eliminate multiple spaces
                	String new_string = received.trim().replaceAll(" +", " ");
                	StringTokenizer str=new StringTokenizer(new_string," ");
                	String argument=str.nextToken();
                	if(argument.equalsIgnoreCase("set"))
                	{
                		this.handleSET(str);
                	}
                	else if(argument.equalsIgnoreCase("get"))
                	{
                		this.handleGET(str);
                	}
                	else if(argument.equalsIgnoreCase("expire"))
                	{
                		this.handleEXPIRE(str);
                	}
                	else if(argument.equalsIgnoreCase("zadd"))
                	{
                		this.handleZADD(str);
                	}
                	else if(argument.equalsIgnoreCase("zrank"))
                	{
                		this.handleZRANK(str);
                	}
                	else if(argument.equalsIgnoreCase("zrange"))
                	{
                		this.handleZRANGE(str);
                	}
                	else
                	{
                		this.dos.writeUTF("We have only design set,get,expire,zadd,zrank,zrange");
                	}

                }
                

            } catch (IOException e) { 
                  
                e.printStackTrace(); 
            } 
              
        } 
        try
        { 
            // closing resources 
            this.dis.close(); 
            this.dos.close(); 
              
        }catch(IOException e){ 
            e.printStackTrace(); 
        } 
 	}
 	public  void handleGET(StringTokenizer str)
 	{
 		String key=str.nextToken();
 		try{
 		if(!str.hasMoreTokens())
 			this.dos.writeUTF(this.map.get(key)+"");
 		else
 			this.dos.writeUTF("Please pass one key at one time");
 		}
 		catch(Exception ex)
 		{
 			System.out.println("Error in handle get");
 		}
 	}
 	public  void handleSET(StringTokenizer str)
 	{
 		try{
 		String key=str.nextToken();
 		String value=str.nextToken();
 		if(!str.hasMoreTokens())
 		{
 		this.map.put(key,value);
 		this.dos.writeUTF("OK");
 		FileOutputStream fos = new FileOutputStream("map.ser");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(this.map);
        oos.close();
    	}
    	else
    	{
    		this.dos.writeUTF("Error Please assign one value to one key");
    	}
 		}
 		catch(Exception ex)
 		{
 			try{
 			this.dos.writeUTF("Please use proper format(like SET key value)");
 			}
 			catch(Exception ex1)
 			{
 				System.out.println("mar ja");
 			}
 		}

 	}
 	public  void handleEXPIRE(StringTokenizer str)
 	{
 		try
 		{
 			String key=str.nextToken();
 			String time=str.nextToken();
 			int int_time=Integer.parseInt(time);
 			if(str.hasMoreTokens())
 			{
 				this.dos.writeUTF("Error");
 				return ;
 			}
 			Timer timer = new Timer();
 			if(this.map.containsKey(key))
 			{

 			this.dos.writeUTF("1");
			timer.schedule(new TimerTask() {
			  
			  public void run() {
			  	try{
			    map.remove(key);
			    FileOutputStream fos = new FileOutputStream("map.ser");
		        ObjectOutputStream oos = new ObjectOutputStream(fos);
		        oos.writeObject(map);
		        oos.close();
		    }
		    catch(Exception exx)
		    {
		    	System.out.println("HEHE");
		    }

			  }
			}, 1000*int_time);
			}
			else
			{
				this.dos.writeUTF("0");
			}



 		}	
 		catch(Exception ex)
 		{
 			try{
 				this.dos.writeUTF("Please use proper format(like Expire key time)");
 			}
 			catch(Exception exx)
 			{
 				System.out.println("mar ja");
 			}
 		}
 	}

 	public void handleZADD(StringTokenizer str)
 	{
 		try{
 		String name=str.nextToken();
 		String score=str.nextToken();
 		float fl_score=Float.parseFloat(score);
 		String value=str.nextToken();
 		if(str.hasMoreTokens())
 		{
 			this.dos.writeUTF("Please enter one score value at one time");
 			return ;
 		}

 		File tempFile = new File(name+".ser");
	    boolean exist=tempFile.exists();
	    tempFile.createNewFile();
	    FileInputStream fis = new FileInputStream(name+".ser");
	    Map mp;
	    if(exist==false)
	    {
	    	mp=new HashMap<Float,Set<String>>();
	    }
	    else
	    {
	    	try{
	          	ObjectInputStream ois = new ObjectInputStream(fis);
       			mp = (Map<Float,Set<String>>) ois.readObject();
       			ois.close();
       			}
       			catch(Exception ex)
       			{
       				System.out.println("phat gya"+ex);
       				return;
       			}
	    }
	    Set<String>st;
	    if(mp.containsKey(fl_score))
	    {
	    	st=(Set<String>)mp.get(fl_score);
	    }
	    else
	    {
	    	st=new HashSet<String>();
	    }
	    st.add(value);
	    mp.put(fl_score,st);
	    FileOutputStream fos = new FileOutputStream(name+".ser");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(mp);
        oos.close();
	    this.dos.writeUTF(mp.size()+"");
 		}
 		catch(Exception ex)
 		{
 			try{
 				this.dos.writeUTF("Please use proper format(like Zadd myset score value)");
 			}
 			catch(Exception exx)
 			{
 				System.out.println("mar ja");
 			}
 		}

 	}
 	public  void handleZRANGE(StringTokenizer str)
 	{
 		try{
 			String name=str.nextToken();
 			String left=str.nextToken();
 			String right=str.nextToken();

 			int left_index=Integer.parseInt(left);
 			int right_index=Integer.parseInt(right);
 			File tempFile = new File(name+".ser");
	   		boolean exist=tempFile.exists();
	   		if(exist==false)
	   		{
	   			this.dos.writeUTF("NIL");
	   			return;
	   		}
	   		Map mp;
	   		FileInputStream fis = new FileInputStream(name+".ser");
	   		ObjectInputStream ois = new ObjectInputStream(fis);
       		mp = (Map<Float,Set<String>>) ois.readObject();
       		ois.close();
       		int size=mp.size();
       		if(left_index<0)
       		{
       			left_index+=size;
       		}
       		if(right_index<0)
       		{
       			right_index+=size;
       		}
       		boolean withscores=false;
       		if(str.hasMoreTokens())
       		{
       			if(str.nextToken().equalsIgnoreCase("withscores"))
       				withscores=true;
       		}

       		int index=0;
       		List<Float>ll=new ArrayList<Float>(mp.keySet());
       		String ans="";
       		for(int i=left_index;i<=right_index;i++)
       		{
       			if(withscores)
       			{
       				ans+=ll.get(i)+"\n";
       			}
       			ans+=mp.get(ll.get(i))+"\n\n";
       		}
       		this.dos.writeUTF(ans);


 		}
 		catch(Exception ex)
 		{
 			try{
 				this.dos.writeUTF("Please use proper format(like zrange myset 0 -1)"+ex);
 			}
 			catch(Exception exx)
 			{
 				System.out.println("mar ja");
 			}
 		}
 	}
 	public  void handleZRANK(StringTokenizer str)
 	{
 		try{
 			String name=str.nextToken();
 			String value=str.nextToken();
 			if(str.hasMoreTokens())
 			{
 				this.dos.writeUTF("Please use proper format(like Zrank myset Value)");
 				return;
 			}

 			File tempFile = new File(name+".ser");
	   		boolean exist=tempFile.exists();
	   		if(exist==false)
	   		{
	   			this.dos.writeUTF("NIL");
	   			return;
	   		}
	   		Map mp;
	   		FileInputStream fis = new FileInputStream(name+".ser");
	   		ObjectInputStream ois = new ObjectInputStream(fis);
       		mp = (Map<Float,Set<String>>) ois.readObject();
       		ois.close();
       		int size=mp.size();
       		List<Float>ll=new ArrayList<Float>(mp.keySet());
       		for(int i=0;i<size;i++)
       		{
       			Set<String> st=(Set<String>)(mp.get(ll.get(i)));
       			if(st.contains(value))
       			{
       				this.dos.writeUTF(ll.get(i)+"");
       				return;
       			}
       		}
       		this.dos.writeUTF("NIL");

 		}
 		catch(Exception ex)
 		{
 			try{
 				this.dos.writeUTF("Please use proper format(like Zrank myset Value)"+ex);
 			}
 			catch(Exception exx)
 			{
 				System.out.println("mar ja");
 			}
 		}
 	}

}