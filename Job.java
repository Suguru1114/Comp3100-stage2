	//
 	// Job class to assist in reading data sent from server
	// This class includes getters for all fields of this class
	//
	public class Job{

		// Variables needed for a job object
		private int startTime;
		private int jobID;
		private int estRunTime;
		private int coreReq;
		private int memoryReq;
		private int diskReq;

		// Job constructor
		public Job(int startTime, int jobID, int estRunTime, int coreReq, int memoryReq, int diskReq){
			this.startTime = startTime;
			this.jobID = jobID;
			this.estRunTime = estRunTime;
			this.coreReq = coreReq;
			this.memoryReq = memoryReq;
			this.diskReq = diskReq;
		}

		public int getStartTime(){
			return this.startTime;
		}
	
		public int getID(){
			return this.jobID;
		}
	
		public int getRunTime(){
			return this.estRunTime;
		}
	
		public int getCoreReq(){
			return this.coreReq;
		}
		
		public int getMemoryReq(){
			return this.memoryReq;
		}
	
		public int getDiskReq(){
			return this.diskReq;
		}
	}