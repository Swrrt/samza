public class StreamSwitch{
    String leaderAddress = null;
    void start(){
        while(leaderAddress == null){

        }
        tryToScale();
    }
    void tryToScale(){
        int startNumber = 1;
        for(int i=startNumber;i<10;i++) {
            try{
                Thread.sleep(10000);
            }catch(Exception e){
            }
            DecisionRMIClient.changeParallelism(leaderAddress, i, null);
        }
    }
}
