public class Server
{
    private String type;
    private int limit;
    private int bootTime;
    private float hourlyRate;
    private int coreCount;
    private int memory;
    private int disk;
    private int id;
    private String state;
    private int startTime;
    private int runTime;
    private int waitTime;
    
    public Server(final String type, final int limit, final int bootTime, final float hourlyRate, final int coreCount, final int memory, final int disk) {
        this.type = type;
        this.limit = limit;
        this.bootTime = bootTime;
        this.hourlyRate = hourlyRate;
        this.coreCount = coreCount;
        this.memory = memory;
        this.disk = disk;
    }
    
    public Server(final String s, final int id, final String state, final int bootTime, final int coreCount, final int memory, final int disk, final int waitTime, final int runTime) {
        this.type = s;
        this.bootTime = bootTime;
        this.coreCount = coreCount;
        this.memory = memory;
        this.disk = disk;
        this.id = id;
        this.state = state;
        this.waitTime = waitTime;
        this.runTime = runTime;
    }
    
    public int getID() {
        return this.id;
    }
    
    public String getState() {
        return this.state;
    }
    
    public int getStartTime() {
        return this.startTime;
    }
    
    public int getWaitTime() {
        return this.waitTime;
    }
    
    public int getRunTime() {
        return this.runTime;
    }
    
    public String getType() {
        return this.type;
    }
    
    public int getLimit() {
        return this.limit;
    }
    
    public int getBootupTime() {
        return this.bootTime;
    }
    
    public Float getHourlyRate() {
        return this.hourlyRate;
    }
    
    public int getCores() {
        return this.coreCount;
    }
    
    public int getMemory() {
        return this.memory;
    }
    
    public int getDisk() {
        return this.disk;
    }
}