package edu.sjsu.cs249.chain;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.sjsu.cs249.raft.*;
import edu.sjsu.cs249.raft.Entry.Builder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class RaftServer extends RaftServerGrpc.RaftServerImplBase {

	int currentTerm;

	HashMap<Long, Integer> votedFor = new HashMap<Long, Integer>();

	int[] nextIndex = new int[4]; //Size of number of servers in Ensembles
	
	private static int localCandidateId=1;//Local CandidateId


	HashMap<Integer, String> log = new HashMap<Integer, String>();
	// main heart beat timeout 10
	// election timeout 1-5 seconds
	//send heartbeats every one sec
	HashMap<Integer, Integer> term = new HashMap<Integer, Integer>();

	int prevLogIndex;

	int prevLogTerm;

	long commitIndex;

	static boolean isLeader;

	boolean isFollower;

	boolean isCandidate;
	
	boolean eligible;

	Server server;

	private int vote;
	
	//static LocalTime heartbeatReceivedTime;

	private static int LeaderId;

	static long electionTimeout;

	static int candidates;

	static boolean heartbeatReceived;

	int lastCommittedIndex;

	ArrayList<Long> candidateList = new ArrayList<Long>();

	private RaftServerGrpc.RaftServerBlockingStub candidateStub;

	File file = new File(
			"/Users/lolithasrestatupadha/Distributed_Comp/cs249/chain/java/src/main/java/edu/sjsu/cs249/chain/config");

	private int getLastLogIndex() {
		return log.isEmpty()?0:log.size();
	}

	private long getLastLogTerm() {
		return term.isEmpty()?0:term.get(term.size()); 
	}

	private Integer getVotedFor(long term) {
		return votedFor.containsKey(term)?votedFor.get(term):-1;
	}

	private void setVotedFor(long term, Integer candidateId) {
		votedFor.put(term, candidateId);
	}

	private long getCurrentTerm() {
		return this.currentTerm;
	}

	private void setCurrentTerm(int cT) {
		this.currentTerm = cT;
	}

	public RaftServer(int port) {
		System.out.println("Starting the server on port " + port);
		server = ServerBuilder.forPort(port).addService(this).build();
		try {
			server.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void appendLogEntries(Entry entry) {
		//Because index according to paper starts from 1
		log.put((int) entry.getIndex(), entry.getDecree());
		term.put((int) entry.getIndex(), (int) entry.getTerm());
	}

	private void deleteLogEntries(int index) {
		for (int i = log.size() - 1; i >= index; i--) {
			log.remove(i);
		}
	}


	private RaftServerGrpc.RaftServerBlockingStub getStub(String session) throws InterruptedException {

		int colon = session.lastIndexOf(':');
		String host = session.substring(0, colon);
		int port = Integer.parseInt(session.substring(colon + 1));
		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext()
				.build();
		return RaftServerGrpc.newBlockingStub(channel);
	}

	public void requestVote(RequestVoteRequest req, StreamObserver<RequestVoteResponse> responseObserver) {

		System.out.println("Received voting request from "+req.getCadidateId());
		RequestVoteResponse rsp;
		if(req.getTerm() > currentTerm) {
			//Update the current term
			currentTerm = (int) req.getTerm();
		}

		if (req.getTerm() < currentTerm)
			rsp = RequestVoteResponse.newBuilder().setTerm(this.currentTerm).setVoteGranted(false).build();
		else if ((getVotedFor(req.getTerm()) == -1 || getVotedFor(req.getTerm()) == req.getCadidateId())
				&& req.getLastLogIndex() >= this.getLastLogIndex()) {
			setVotedFor(req.getTerm(), req.getCadidateId());
			System.out.println("Voting for "+req.getCadidateId()+" for the term "+req.getTerm());
			rsp = RequestVoteResponse.newBuilder().setTerm(this.currentTerm).setVoteGranted(true).build();
		} else {
			System.out.println("Not Voting for "+req.getCadidateId()+" for the term "+req.getTerm());
			rsp = RequestVoteResponse.newBuilder().setTerm(this.currentTerm).setVoteGranted(false).build();
		}

		responseObserver.onNext(rsp);
		responseObserver.onCompleted();
	}
	
	private void insertInTable(Builder entry) {
		 String sql = "INSERT INTO records(id, decree, term) VALUES(?,?,?)";  
		 long index = entry.getIndex();
		 String decree = entry.getDecree();
		 long term = entry.getTerm();
	        try{  
	            Connection conn = this.connect();  
	            PreparedStatement pstmt = conn.prepareStatement(sql);  
	            pstmt.setLong(1, index);  
	            pstmt.setString(2, decree);  
	            pstmt.setLong(3, term);  
	            pstmt.executeUpdate(); 
	            System.out.println("Inserted the entry into the table");
	        } catch (SQLException e) {  
	            System.out.println(e.getMessage());  
	        }  
	}
	
    private Connection connect() {  
        // SQLite connection string  
        String url = "jdbc:sqlite:DC.db";  
        Connection conn = null;  
        try {  
            conn = DriverManager.getConnection(url);  
        } catch (SQLException e) {  
            System.out.println(e.getMessage());  
        }  
        return conn;  
    }  

	public void appendEntries(AppendEntriesRequest req, StreamObserver<AppendEntriesResponse> responseObserver) {

		// Invoked by leaders
		//LocalTime heartbeatReceivedTime = java.time.LocalTime.now();
		if(req.getTerm() >= currentTerm) {
			heartbeatReceived = true;
			LeaderId = req.getLeaderId();	
		}
		AppendEntriesResponse rsp;
		System.out.println("Leader is "+req.getLeaderId()+" for term "+req.getTerm());
		if(!req.hasEntry()) {
			if(req.getTerm() >= currentTerm) {
				isLeader = false;
				isFollower = true;
				isCandidate = false;
				System.out.println("Updating currentTerm from heartbeat");
			}
			System.out.println("Received Heartbeat from "+req.getLeaderId());
			rsp = AppendEntriesResponse.newBuilder().setTerm(this.currentTerm).setSuccess(true).build();
			responseObserver.onNext(rsp);
			responseObserver.onCompleted();
			return;
		}
		System.out.println("Received Append Entries request");
		if(req.getTerm() > currentTerm) {
			setCurrentTerm((int)req.getTerm());
			isLeader = false; //Leaders will not get this request any time. But just making it false again
			isFollower = true;
			isCandidate = false;
			System.out.println("Updating currentTerm from appendEntries");
		}
		if (req.getTerm() < currentTerm) {
			rsp = AppendEntriesResponse.newBuilder().setTerm(this.currentTerm).setSuccess(false).build();
			System.out.println("My Current term is greater than reqest term, hence rejecting the request");
		} else if (term.containsKey(req.getPrevLogIndex())
				&& (term.get(req.getPrevLogIndex()) != req.getPrevLogTerm())) { // check Null pointer
			rsp = AppendEntriesResponse.newBuilder().setTerm(this.currentTerm).setSuccess(false).build();
			System.out.println("Log inconsistency, hence rejecting the append request");
		} 
//		else if (req.getPrevLogIndex() != this.prevLogIndex || req.getPrevLogTerm() != this.prevLogTerm) {
//			rsp = AppendEntriesResponse.newBuilder().setTerm(this.currentTerm).setSuccess(false).build();
//			System.out.println("Log inconsistancy, hence rejecting the request");
//		} 
		else if (log.containsKey((int) req.getEntry().getIndex())) {
			if ( (term.get(req.getEntry().getIndex()) != req.getEntry().getTerm()))
				deleteLogEntries((int) req.getEntry().getIndex());
			appendLogEntries(req.getEntry());
			insertInTable(req.getEntry().toBuilder());
			rsp = AppendEntriesResponse.newBuilder().setTerm(this.currentTerm).setSuccess(true).build();
		} else {
			appendLogEntries(req.getEntry());
			insertInTable(req.getEntry().toBuilder());
			rsp = AppendEntriesResponse.newBuilder().setTerm(this.currentTerm).setSuccess(true).build();
		}

		if (req.getLeaderCommit() > commitIndex) {
			commitIndex = Math.min(req.getLeaderCommit(), log.size() - 1);
			System.out.println("Updating the commit index to "+commitIndex);
		}
		
		System.out.println("Log after append Entries");
		for (Integer name: log.keySet()){
            Integer key = name;
            String value = log.get(name).toString();  
            System.out.println(key + " " + value);  
        }

		responseObserver.onNext(rsp);
		responseObserver.onCompleted();
	}

	private void startElection() {
		// Did not receive heart beat, start election
		setCurrentTerm((int) (getCurrentTerm() + 1));
		int candidateId;
		electionTimeout = (long) (5000*Math.random());
		while(electionTimeout == 0) {
			electionTimeout = (long) (5000*Math.random());
		}
		isCandidate = true;
		isLeader = false;
		isFollower = false;
		vote = 1; // Voted for self
		votedFor.put((long)currentTerm, localCandidateId);
		RequestVoteRequest.Builder request = RequestVoteRequest.newBuilder();
		request.setTerm(getCurrentTerm());
		request.setCadidateId(localCandidateId);
		request.setLastLogIndex(getLastLogIndex());
		request.setLastLogTerm(getLastLogTerm());

		// File file = new
		Scanner fileRead;
		try {
			fileRead = new Scanner(file);
			candidates = 0;
			ExecutorService es = Executors.newCachedThreadPool();
			eligible = true;
			while (fileRead.hasNextLine() && !heartbeatReceived && eligible) {
				//System.out.println("Requesting vote while loop again");
				String data = fileRead.nextLine();
				int colon = data.lastIndexOf(' ');
				candidates++;
				candidateId = Integer.parseInt(data.substring(0, colon));
				//System.out.println("Requesting stub for "+data.substring(colon + 1));
				if(candidateId != localCandidateId) {
				candidateStub = getStub(data.substring(colon + 1));
				System.out.println("Requesting for the vote to "+candidateId);
				
				es.execute(new Runnable() {
					@Override
					public void run() {
						try {
							RequestVoteResponse rsp = candidateStub.requestVote(request.build());
							if (rsp.getTerm() > currentTerm) {
								currentTerm = (int) rsp.getTerm();
								isLeader = false;
								isCandidate = false;
								isFollower = true;
								eligible = false;
								System.out.println("My term is not up to date. Becoming Follower");
								return;
							}
							if (rsp.getVoteGranted())
								vote++;
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
				}
			}
			es.shutdown();
			//es.awaitTermination(electionTimeout, TimeUnit.SECONDS);
			Thread.sleep(electionTimeout);

//			if (heartbeatReceived) {
//				System.out.println("Heartbeat received stopping election");
//				isFollower = true;
//				isLeader = false;
//				isCandidate = false;
//				return;
//			}
//			if (vote >= (candidates / 2)+1) {
//				System.out.println("Received majority of "+candidates+" becoming the Leader");
//				isLeader = true;
//				isCandidate = false;
//				return;
//			}

			//es.awaitTermination(electionTimeout, TimeUnit.SECONDS);
			//Thread.sleep(electionTimeout);

			// Thread.sleep(electionTimeout);// start new election
			if (heartbeatReceived) {
				System.out.println("Hearbeat received. Becoming Follower");
				isFollower = true;
				isLeader = false;
				isCandidate = false;
				return;
			} else if (vote > (candidates / 2)+1) {
				System.out.println("Won the Election. Becoming Leader");
				//setCurrentTerm(currTerm);
				isLeader = true;
				isCandidate = false;
				return;
			} else {
				System.out.println("Election timedout. Starting new election");
				startElection();
			}
			// if election timeout elapses start new election
		} catch (FileNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private AppendEntriesRequest.Builder buildAppendEntriesRequest(int followerIndex, int nextIndex) {
		AppendEntriesRequest.Builder request = AppendEntriesRequest.newBuilder();

		Entry.Builder entry = Entry.newBuilder();
		entry.setIndex(nextIndex);
		entry.setTerm(term.get(nextIndex));
		entry.setDecree(log.get(nextIndex));
		request.setTerm(getCurrentTerm());
		request.setLeaderId(LeaderId);
		// when sending old entries should we also decrement this
		request.setPrevLogIndex(nextIndex - 1);
		request.setPrevLogTerm(term.get(nextIndex - 1));
		request.setLeaderCommit(commitIndex);
		request.setEntry(entry.build());
		return request;
	}
	
	private int getLatestIndex() {
		return log.isEmpty()?1:log.size()+1;
	}

	public void clientAppend(ClientAppendRequest req, StreamObserver<ClientAppendResponse> responseObserver) {
		ClientAppendResponse rsp;
		// AppendEntriesResponse response;
		System.out.println("Received ClientAppend Request");
		if (isLeader) {
			int index = getLatestIndex();
			log.put(index,req.getDecree());
			term.put(index, currentTerm);
			
			Entry.Builder entry = Entry.newBuilder();
			entry.setIndex(index);
			entry.setTerm(term.get(index));
			entry.setDecree(req.getDecree());
			insertInTable(entry);
			
			// Send AppendEntries RPC to all the servers
			AppendEntriesRequest.Builder request = AppendEntriesRequest.newBuilder();
			prevLogTerm = index==1?0:term.get(index-1);
			request.setTerm(getCurrentTerm());
			request.setLeaderId(LeaderId);
			request.setPrevLogIndex(index-1);
			request.setPrevLogTerm(prevLogTerm);
			request.setLeaderCommit(commitIndex);
			request.setEntry(entry.build());

			Scanner fileRead;
			try {
				fileRead = new Scanner(file);
				ExecutorService es = Executors.newCachedThreadPool();
				while (fileRead.hasNextLine()) {
					String data = fileRead.nextLine();
					int colon = data.lastIndexOf(' ');
					long cid = Long.parseLong(data.substring(0, colon));
					int followerIndex;
					if(cid != localCandidateId) {
					followerIndex = candidateList.indexOf(cid);
					candidateStub = getStub(data.substring(colon + 1));
					// response = candidateStub.appendEntries(request.build());
					es.execute(new Runnable() {
						@Override
						public void run() {
							try {
								AppendEntriesResponse response = candidateStub.appendEntries(request.build());
								int flag = 0;
								while (!response.getSuccess()) {
									flag = 1;
									// Decrease nextIndex and send the log again
									// followerIndex is to fetch nextIndex and send accordingly
									nextIndex[followerIndex] = nextIndex[followerIndex] - 1;
									AppendEntriesRequest.Builder req = buildAppendEntriesRequest(followerIndex,
											nextIndex[followerIndex]);
									response = candidateStub.appendEntries(req.build());
									// It breaks when response becomes success
									// Then we need to send all the next logs
								}
								if (flag == 1) {
									// Increment its next index and send all logs
									while (nextIndex[followerIndex] <= log.size() - 1) {
										nextIndex[followerIndex] = nextIndex[followerIndex] + 1;
										AppendEntriesRequest.Builder req = buildAppendEntriesRequest(followerIndex,
												nextIndex[followerIndex]);
										response = candidateStub.appendEntries(req.build());
									}
								}

							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					});

				}
			  }
			} catch (FileNotFoundException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			// es.awaitTermination(electionTimeout, TimeUnit.SECONDS);
			commitIndex = index;
			System.out.println("Log after Client Append");
			for (Integer name: log.keySet()){
	            Integer key = name;
	            String value = log.get(name).toString();  
	            System.out.println(key + " " + value);  
	        }
			
			rsp = ClientAppendResponse.newBuilder().setRc(0).setLeader(localCandidateId).setIndex(log.size()).build();
		} else {
			rsp = ClientAppendResponse.newBuilder().setRc(1).setLeader(LeaderId).setIndex(log.size()).build();
		}

		responseObserver.onNext(rsp);
		responseObserver.onCompleted();

	}

	public void clientRequestIndex(ClientRequestIndexRequest req,
			StreamObserver<ClientRequestIndexResponse> responseObserver) {
		ClientRequestIndexResponse rsp;
		String decree;
		System.out.println("Received request Index from client");
		if (isLeader) {
			if (req.getIndex() < commitIndex) {
				decree = log.get((int) req.getIndex());
			} else {
				decree = log.get((int)commitIndex);
			}
			System.out.println("Since I am the leader, responding with decree");
			rsp = ClientRequestIndexResponse.newBuilder().setRc(0).setLeader(localCandidateId).setIndex(req.getIndex())
					.setDecree(decree).build();
		} else {
			System.out.println("Since I am not the leader, responding with LeaderId");
			rsp = ClientRequestIndexResponse.newBuilder().setRc(1).setLeader(LeaderId).build();
		}

		responseObserver.onNext(rsp);
		responseObserver.onCompleted();
	}

	private void sendHeartBeats() {
		AppendEntriesRequest.Builder request = AppendEntriesRequest.newBuilder();
		request.setTerm(getCurrentTerm());
		request.setLeaderId(LeaderId);
		request.setPrevLogIndex(prevLogIndex);
		request.setPrevLogTerm(prevLogTerm);
		request.setLeaderCommit(commitIndex);
		//Entry.Builder entry = Entry.newBuilder();
		//entry.setIndex(1);
		//entry.setTerm(term.get(nextIndex));
		//entry.setDecree(log.get(nextIndex));
		//request.setEntry(entry);

		Scanner fileRead;
		try {
			fileRead = new Scanner(file);
			while (fileRead.hasNextLine()) {
				AppendEntriesResponse rsp;
				String data = fileRead.nextLine();
				int colon = data.lastIndexOf(' ');
				int candidateId = Integer.parseInt(data.substring(0, colon));
				if(candidateId != localCandidateId) {
				System.out.println("Sending heartbeats to candidateId "+candidateId);
				rsp = getStub(data.substring(colon + 1)).appendEntries(request.build());
				}
			}
		} catch (FileNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void initializeNextIndex() {
		Scanner fileRead;
		if(!log.isEmpty()) {
		prevLogIndex = log.size();
		prevLogTerm = term.get(prevLogIndex);
		}
		try {
			fileRead = new Scanner(file);
			// candidates = 0;
			int i = 0;
			while (fileRead.hasNextLine()) {
				String data = fileRead.nextLine();
				int colon = data.lastIndexOf(' ');
				// candidateList[i] = Long.parseLong(data.substring(0, colon));
				candidateList.add(i, Long.parseLong(data.substring(0, colon)));
				//matchIndex[i] = 0;
				if(log.isEmpty())
					nextIndex[i] = 1;
				else
					nextIndex[i] = log.size()+1;
				i++;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void createNewTable() {  
        // SQLite connection string  
        String url = "jdbc:sqlite:DC.db";  
        
		try {
			Connection connection = DriverManager.getConnection(url);
			if (connection != null) {
				//	DatabaseMetaData meta = connection.getMetaData();
					//System.out.println("A new database has been created.");
				}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		 String sql_drop = "DROP TABLE IF EXISTS records";  
		 
          
        // SQL statement for creating a new table  
        String sql = "CREATE TABLE IF NOT EXISTS records (\n"  
                + " id integer PRIMARY KEY,\n"  
                + " decree text,\n"  
                + " term integer\n"  
                + ");";  
          
        try{  
        	Connection connection = DriverManager.getConnection(url);
            Statement stmt = connection.createStatement();
           // stmt.execute(sql_drop); //For testing purpose, If I want to clear db.
            stmt.execute(sql); 
            System.out.println("Database table has been created.");
        } catch (SQLException e) {  
            System.out.println(e.getMessage());  
        }  
    }  

	public static void main(String[] args) {
		RaftServer raft = new RaftServer(4444);
		raft.createNewTable();
		raft.initializeValues();
		raft.isFollower = true;
		System.out.println("Starting the server. I am the Follower");
		try {
			Thread.sleep(10000);//waits for 10 seconds
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while (true) {
			if (isLeader) {
				System.out.println("I am the Leader for the term "+raft.currentTerm);
				LeaderId = localCandidateId;
				raft.sendHeartBeats();
			    raft.initializeNextIndex();
			}
			try {
				heartbeatReceived = false;
				System.out.println("Current term is "+raft.currentTerm);
				Thread.sleep(5000);//random timeout
			if (!isLeader && !heartbeatReceived) {
				heartbeatReceived = false;
			    System.out.println("Heartbeat not received. Starting election");
			    raft.startElection();
			}
			}
			 catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void initializeValues() {
		//Fetch data from db
		fetch();
		if (log.isEmpty()) {
			currentTerm = 0;
			commitIndex = 0;
			prevLogIndex= 0;
			prevLogTerm = 0;
		}else {
			commitIndex = log.size();
			currentTerm = term.get(log.size());
			prevLogIndex= log.size();
			prevLogTerm = term.get(log.size());
			
		}
	}
	
	public void fetch() {
		String sql = "SELECT id, decree, term from records order by id ";
		String decree = " ";
		int id;
		int trm;
		System.out.println("IN FETCH");
		try {
			String url = "jdbc:sqlite:DC.db";
			Connection connection = null;
			try {
				connection = DriverManager.getConnection(url);
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
			PreparedStatement statement = connection.prepareStatement(sql);
			//statement.setString(1, id);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				decree = rs.getString("decree"); 
				id = rs.getInt("id");
				trm = rs.getInt("term");
				log.put(id, decree);
				term.put(id,trm);
				
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		System.out.println("Data restored in log");

	}
}
