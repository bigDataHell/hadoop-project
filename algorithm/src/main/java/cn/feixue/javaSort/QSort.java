package cn.feixue.javaSort;

import java.util.Arrays;

/**
 * 用java实现的快速排序
 */
public class QSort {

    public static void main(String[] args) {

        int[] arr = {4,3,6,45,2,1,89,-1,9,0};


        qSort(arr);

        System.out.println(Arrays.toString(arr));

    }

    private static void qSort(int[] arr) {

        if (arr.length > 0 ) {
            qSort(arr, 0, arr.length - 1);
        }

    }

    private static void qSort(int[] arr, int low, int high) {
        // 递归算法的出口
        if (low > high) {
            return;
        }

        // 定义两个指针
        int i = low;
        int j = high;

        // key
        int key = arr[low];
        System.out.println("key : "+key);

        // 完成一趟排序
        while (i < j) {
            // 从右往左找小于key的数
            while (i < j && arr[j] > key) {
                j--;
            }
            // 从左往右找大于key的数
            while (i < j && arr[i] <= key ){
                i++;
            }

            // 交换
            if (i < j) {
                int temp = arr[i];
                arr[i] = arr[j];
                arr[j] = temp;
            }
        }

        // 调整key的位置
        int temp = arr[i];
        arr[i] = arr[low];
        arr[low] = temp;

        // 对key左边的排序
        qSort(arr, low, i - 1);
        // 对key右边的排序
        qSort(arr, i + 1, high);
    }



}
