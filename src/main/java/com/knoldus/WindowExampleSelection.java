package com.knoldus;

import java.util.Scanner;

/**
 * WindowExampleSelection gives user flexibility to select a Flink window assigner application.
 */
public final class WindowExampleSelection {

    public static void main(String[] args) {
        System.out.println("--------------------------------------------------");
        System.out.println("Press 1 for Tumbling window");
        System.out.println("Press 2 for Sliding window");
        System.out.println("Press 3 for Count window");
        System.out.println("Press 4 for Session window");
        System.out.println("---------------------------------------------------");


        System.out.println("Enter a Number for which window operation want to execute");
        final Scanner myInput = new Scanner( System.in );

        switch(myInput.nextInt()) {

            case 1: {
                System.out.println("Running Tumbling window application");
                TumblingWindows tumblingWindows = new TumblingWindows();
                tumblingWindows.tumblingWindow();
            }

            case 2: {
                System.out.println("Running Sliding window application");
                SlidingWindows slidingWindows = new SlidingWindows();
                slidingWindows.slidingWindow();
            }

            case 3: {
                System.out.println("Running Count window application");
                CountWindows countWindows = new CountWindows();
                countWindows.countWindow();
            }

            case 4: {
                System.out.println("Running Session window application");
                SessionWindow sessionWindow = new SessionWindow();
                sessionWindow.sessionWindow();
            }

            default:
                System.out.println("No such operation available for this number, please enter only " +
                        "provided numbers");
                break;
        }

    }

}
