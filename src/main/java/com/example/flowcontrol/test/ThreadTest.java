package com.example.flowcontrol.test;

import java.util.concurrent.atomic.AtomicInteger;

public class ThreadTest implements Runnable{
    private AtomicInteger num = new AtomicInteger(0);

    public AtomicInteger getNum() {
        return num;
    }

    public void setNum(AtomicInteger num) {
        this.num = num;
    }

    public static void main(String[] args) {
        ThreadTest inc = new ThreadTest();
        for (int i = 1; i <= 10000; i++){
            new Thread(inc).start();
        }
//        System.out.println(inc.getNum());
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        for(int i=1;i<=20;i++){

            try {
                num.getAndIncrement();
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(num.get());
    }
}
