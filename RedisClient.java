import java.io.*;
import java.net.*;
import java.util.*;
public class RedisClient
{
	private ServerSocket server;
	private Socket socket;
	private DataInputStream inn;
	private DataOutputStream out;

	public  RedisClient(String address,int port)
	{
		try
        { 
            socket = new Socket(address, port); 
            System.out.println("Connected"); 
  
            Scanner sc=new Scanner(System.in);
            inn=new DataInputStream(socket.getInputStream());
            out    = new DataOutputStream(socket.getOutputStream()); 
        	String line = "";  
        while (true) 
        { 
            try
            {   line=sc.nextLine();
                out.writeUTF(line);
                out.flush();
                System.out.println(inn.readUTF()); 
            } 
            catch(IOException i) 
            { 
                System.out.println(i); 
            }
            if(line.equalsIgnoreCase("exit"))
            	break; 
        }
        System.out.println(inn.readUTF());  
        } 
        catch(UnknownHostException u) 
        { 
            System.out.println(u); 
        } 
        catch(IOException i) 
        { 
            System.out.println(i); 
        } 
 
        try
        { 
            
            out.close(); 
            socket.close(); 
        } 
        catch(IOException i) 
        { 
            System.out.println(i); 
        } 
	}
	public static void main(String[] args) {
		String address;
		int port;
		Scanner sc=new Scanner(System.in);
		System.out.println("Please enter Server IP and their port");
		address=sc.nextLine();
		port=sc.nextInt();
		RedisClient redisClient=new RedisClient(address,port);
	}


}