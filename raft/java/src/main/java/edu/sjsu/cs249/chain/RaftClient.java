package edu.sjsu.cs249.chain;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.Scanner;

import edu.sjsu.cs249.raft.ClientAppendRequest;
import edu.sjsu.cs249.raft.ClientAppendResponse;
import edu.sjsu.cs249.raft.ClientRequestIndexRequest;
import edu.sjsu.cs249.raft.ClientRequestIndexResponse;
import edu.sjsu.cs249.raft.RaftServerGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class RaftClient {
	
	//private ManagedChannel channel;
    private RaftServerGrpc.RaftServerBlockingStub  blockingStub;
    long LeaderId;
    String session;
    File file = new File(
			"/Users/lolithasrestatupadha/Distributed_Comp/cs249/chain/java/src/main/java/edu/sjsu/cs249/chain/config");
    Scanner fileRead;
	private static Scanner scanner;

    
    private static InetSocketAddress str2addr(String addr) {
		int colon = addr.lastIndexOf(':');
		return new InetSocketAddress(addr.substring(0, colon), Integer.parseInt(addr.substring(colon + 1)));
	}
    
    private RaftServerGrpc.RaftServerBlockingStub getStub(String session) throws InterruptedException {
    	//InetSocketAddress addr = str2addr(new String(session).split("\n")[0]);
    			int colon = session.lastIndexOf(':');
    			String host = session.substring(0, colon);
    			int port = Integer.parseInt(session.substring(colon + 1));
    			System.out.println("Sending request to  "+port);
    			ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext()
    					.build();
    			return RaftServerGrpc.newBlockingStub(channel);
	}
    
    private void readLeaderId() {
    	try {
			fileRead = new Scanner(file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	while (fileRead.hasNextLine()) {
			String data = fileRead.nextLine();
			int colon = data.lastIndexOf(' ');
		    LeaderId = Integer.parseInt(data.substring(0, colon));
			session = data.substring(colon + 1);
    	}
    }
    
    private String getLeaderSession(long lid){
    	try {
			fileRead = new Scanner(file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	while (fileRead.hasNextLine()) {
			String data = fileRead.nextLine();
			int colon = data.lastIndexOf(' ');
		    LeaderId = Integer.parseInt(data.substring(0, colon));
		    if(LeaderId == lid) {
		    	session = data.substring(colon + 1);
		    	break;
		    }
    	}
    	
    	return session;
    }
    
    private void ClientRequestIndex(int index) {
    	if(LeaderId == -1) {
			readLeaderId();
		}
    	try {
			blockingStub = getStub(session);
			ClientRequestIndexResponse rsp = blockingStub.clientRequestIndex(ClientRequestIndexRequest.newBuilder().setIndex(index).build());
			if(rsp.getRc()==1) {//Not the leader
				LeaderId = rsp.getLeader();
				getLeaderSession(LeaderId);
				blockingStub = getStub(session);
				ClientRequestIndexResponse response = blockingStub.clientRequestIndex(ClientRequestIndexRequest.newBuilder().setIndex(index).build());
                if(response.getRc() == 0) 
                	System.out.println("Value retrived "+response.getDecree());
			}else {
				System.out.println("Value retrived "+rsp.getDecree());
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    
	private void clientAppendRequest(String decree) {
		if(LeaderId == -1) {
			readLeaderId();
		}
		try {
			blockingStub = getStub(session);
			ClientAppendResponse rsp = blockingStub.clientAppend(ClientAppendRequest.newBuilder().setDecree(decree).build());
			if(rsp.getRc()==1) {//Not the leader
				LeaderId = rsp.getLeader();
				System.out.println("Current Leader is "+rsp.getLeader());
				getLeaderSession(LeaderId);
				blockingStub = getStub(session);
				System.out.println("Sending Append request to Leader "+rsp.getLeader());
				ClientAppendResponse response = blockingStub.clientAppend(ClientAppendRequest.newBuilder().setDecree(decree).build());
                if(response.getRc() == 0) 
                	System.out.println("Value sucessfully stored in log at index "+response.getIndex());
			}else {
				System.out.println("Value sucessfully stored in log at index "+rsp.getIndex());
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	 public static void main(String args[]) throws Exception {
		    RaftClient client = new RaftClient(); 
		    client.LeaderId=-1;
	        scanner = new Scanner(System.in);
	        String line;
	        while ((line = scanner.nextLine()) != null) {
	            String parts[] = line.split(" ");
	            System.out.print(parts[0]);
	            if (parts[0].equals("put")) {
	                client.clientAppendRequest(parts[1]);
	            } else if (parts[0].equals("get")) {
	                client.ClientRequestIndex(Integer.parseInt(parts[1]));
	            } else {
	                System.out.println("don't know " + parts[0]);
	                System.out.println("i know:");
	                System.out.println("get key");
	                System.out.println("inc key value");
	                System.out.println("del key");
	            }
	        }
	    }
}
