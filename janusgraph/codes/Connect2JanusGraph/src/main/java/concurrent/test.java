package concurrent;

public class test {

    public static void main(String[] args) throws InterruptedException {

        int num=3;
        Object lock=new Object();
        for (int i = 0; i < num; i++) {
            synchronized (lock){
                Thread t1=new Thread(new Query(i+""));
                t1.start();

                t1.join(200);
                t1.interrupt();
            }
        }
    }

}

class Query implements Runnable{

    private String root;

    public Query(String root) {
        this.root = root;
    }

    public void run() {
        while (true){
            Thread current = Thread.currentThread();
            if(current.isInterrupted()){
                System.out.println("===打断"+root+"===");
                break;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                current.interrupt();
            }
        }
    }
}
