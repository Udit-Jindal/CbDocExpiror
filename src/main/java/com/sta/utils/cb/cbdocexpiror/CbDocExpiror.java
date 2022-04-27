package com.sta.utils.cb.cbdocexpiror;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.sta.utils.dfs.FsUtils;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class CbDocExpiror
{

    public static void main(String[] args) throws Exception
    {
        if ((args == null) || (args.length == 0) || (args[0] == null) || (args[0].isEmpty()))
        {
            System.out.println("This program needs atleast one argument. Config File Path is missing.");
        }

        JSONObject jsonConfig = FsUtils.readJsonFile(args[0]);

        //<editor-fold defaultstate="collapsed" desc="Read Cb Config">
        JSONArray cbIps;

        try
        {
            cbIps = (JSONArray) jsonConfig.get("cbIp");
        }
        catch (Exception ex)
        {
            System.out.println("Error in parsing cbIp parameter. Expected Array.");
            System.out.println("Exception: " + ex);

            return;
        }

        if ((cbIps == null) || cbIps.isEmpty())
        {
            System.out.println("Cb Ips missing. Exiting.");
            return;
        }

        String[] cbIpArray = new String[cbIps.size()];
        int i = 0;
        for (Object cbIp : cbIps)
        {
            cbIpArray[i++] = (String) cbIp;
        }

        String cbUser = (String) jsonConfig.get("cbUser");
        if (cbUser == null)
        {
            System.out.println("Cb User missing. Exiting.");
            return;
        }

        String cbPassword = (String) jsonConfig.get("cbPassword");
        if (cbPassword == null)
        {
            System.out.println("Cb Password missing. Exiting.");
            return;
        }

        String cbBucketName = (String) jsonConfig.get("cbBucket");
        if (cbBucketName == null)
        {
            System.out.println("Cb Bucket missing. Exiting.");
            return;
        }

        String cbDesignDocName = (String) jsonConfig.get("cbDesignDocumentName");
        if (cbDesignDocName == null)
        {
            System.out.println("Cb Design Documnet Name missing. Exiting.");
            return;
        }

        String cbViewName = (String) jsonConfig.get("cbViewName");
        if (cbViewName == null)
        {
            System.out.println("Cb View Name missing. Exiting.");
            return;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc="Read Date Config">
        String fromDateString = (String) jsonConfig.get("fromDate");
        if ((fromDateString == null) || fromDateString.isEmpty())
        {
            System.out.println("From Date missing. Exiting.");
            return;
        }

        String[] fromDateArray = fromDateString.split("-");
        if (fromDateArray.length != 3)
        {
            System.out.println("Invalid from date. Exiting." + fromDateString);
            return;
        }

        Integer fromHour;
        if (jsonConfig.get("fromHour") == null)
        {
            fromHour = 0;
        }
        else
        {
            fromHour = ((Long) jsonConfig.get("fromHour")).intValue();
        }

        String toDateString = (String) jsonConfig.get("toDate");
        if ((toDateString == null) || toDateString.isEmpty())
        {
            System.out.println("To Date missing. Exiting.");
            return;
        }

        String[] toDateArray = toDateString.split("-");
        if (toDateArray.length != 3)
        {
            System.out.println("Invalid to date. Exiting." + toDateString);
            return;
        }

        Integer toHour;
        if (jsonConfig.get("toHour") == null)
        {
            toHour = 0;
        }
        else
        {
            toHour = ((Long) jsonConfig.get("toHour")).intValue();
        }

        Calendar currentTime = Calendar.getInstance();
        currentTime.set(Calendar.YEAR, Integer.parseInt(fromDateArray[0]));
        currentTime.set(Calendar.MONTH, Integer.parseInt(fromDateArray[1]) - 1);
        currentTime.set(Calendar.DATE, Integer.parseInt(fromDateArray[2]));
        currentTime.set(Calendar.HOUR_OF_DAY, fromHour);
        currentTime.set(Calendar.MINUTE, 0);
        currentTime.set(Calendar.SECOND, 0);
        Long startKey = currentTime.getTimeInMillis();

        currentTime.set(Calendar.YEAR, Integer.parseInt(toDateArray[0]));
        currentTime.set(Calendar.MONTH, Integer.parseInt(toDateArray[1]) - 1);
        currentTime.set(Calendar.DATE, Integer.parseInt(toDateArray[2]));
        currentTime.set(Calendar.HOUR_OF_DAY, toHour);
        currentTime.set(Calendar.MINUTE, 59);
        currentTime.set(Calendar.SECOND, 59);
        currentTime.set(Calendar.SECOND, 59);
        Long endKey = currentTime.getTimeInMillis();
        //</editor-fold>

        Integer batchSize = ((Long) jsonConfig.get("batchSize")).intValue();
        Integer threadCnt = ((Long) jsonConfig.get("threads")).intValue();

        String monitorDocId = (String) jsonConfig.get("monitorDocId");

        Boolean autoMode;

        if (jsonConfig.get("autoMode") != null)
        {
            autoMode = (Boolean) jsonConfig.get("autoMode");
        }
        else
        {
            autoMode = false;
        }

        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("**************************Starting Data Purge For Config::******************************");
        System.out.println("********************************************************************************************");
        System.out.println("");

        System.out.println("Cb Ips: " + Arrays.toString(cbIpArray));

        System.out.println("Cb Bucket: " + cbBucketName);

        System.out.println("Cb Design Doc Name: " + cbDesignDocName);
        System.out.println("Cb View Name: " + cbViewName);

        System.out.println("Batch Size: " + batchSize);
        System.out.println("Threads: " + threadCnt);
        System.out.println("Monitor Doc Id: " + monitorDocId);
        System.out.println("Auto Mode: " + autoMode);

        TimeUnit.SECONDS.sleep(10);

        while (true)
        {
            DbManager db = new DbManager(cbIpArray, cbUser, cbPassword);

            Bucket cbBucket = db.getBucket(cbBucketName);

            long startHourKey;
            long endHourKey;

            if (autoMode)
            {
                startHourKey = 1546300800000l;
                endHourKey = getArchiveInfo(cbBucket, monitorDocId);
            }
            else
            {
                startHourKey = startKey;
                endHourKey = endKey;
            }

            System.out.println("");
            System.out.println("");
            System.out.println("");
            System.out.println("");
            System.out.println("");

            Calendar calTemp = Calendar.getInstance();
            calTemp.setTimeInMillis(startHourKey);
            System.out.println("From Date: " + calTemp.get(Calendar.YEAR) + "-" + (calTemp.get(Calendar.MONTH) + 1) + "-" + calTemp.get(Calendar.DATE) + ":" + calTemp.get(Calendar.HOUR_OF_DAY));

            calTemp = Calendar.getInstance();
            calTemp.setTimeInMillis(endHourKey);
            System.out.println("To Date: " + calTemp.get(Calendar.YEAR) + "-" + (calTemp.get(Calendar.MONTH) + 1) + "-" + calTemp.get(Calendar.DATE) + ":" + calTemp.get(Calendar.HOUR_OF_DAY));

            System.out.println("**************************************Delete data START************************************************");

            TimeRangeViewReaderDelete viewReader = new TimeRangeViewReaderDelete(cbBucket, cbDesignDocName, cbViewName, startHourKey, endHourKey);

            CountDownLatch latch = new CountDownLatch(threadCnt);

            ExecutorService threadPool = Executors.newFixedThreadPool(threadCnt);

            //<editor-fold defaultstate="collapsed" desc="Delete Docs">
            for (int counter = 0; counter < threadCnt; counter++)
            {
                DataDeleteThread dataWorker = new DataDeleteThread(cbBucket, viewReader, latch, batchSize);

                threadPool.execute(dataWorker);
            }
            //</editor-fold>

            latch.await();

            cbBucket.close();
            db.close();

            if (viewReader.isEmpty() && autoMode)
            {
                System.out.println("Reader Empty. Waiting for 5 minutes.");

                TimeUnit.MINUTES.sleep(5);

                System.out.println("Wait time over. Restarting now.");
            }

            if (viewReader.isEmpty() && !autoMode)
            {
                System.out.println("**************************************Delete data FINISH************************************************");
                break;
            }
        }
    }

    private static Long getArchiveInfo(Bucket cbBucket, String monitorDocId)
    {
        Calendar date = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        JsonDocument doc = cbBucket.get(monitorDocId, 30, TimeUnit.SECONDS);

        JsonObject json = doc.content();

        date.set(json.getInt("year"), json.getInt("month") - 1, json.getInt("date"), json.getInt("hour"), 0, 0);// -1 in month, since January = 0.

        date.set(Calendar.MILLISECOND, 0); //reset

        return date.getTimeInMillis();
    }

    static class DataDeleteThread extends Thread
    {

        //variables
        private final Bucket _bucket;
        private final TimeRangeViewReaderDelete _reader;

        private final CountDownLatch _latch;

        private final Integer _batchSize;

        Exception _threadException;

        //constructor
        public DataDeleteThread(Bucket bucket, TimeRangeViewReaderDelete reader, CountDownLatch latch, Integer batchSize)
        {
            _bucket = bucket;
            _reader = reader;

            _latch = latch;
            _batchSize = batchSize;
        }

        //public
        @Override
        public void run()
        {

            while (!_reader.isEmpty())
            {
                try
                {
                    List<String> docIds = _reader.getNext(_batchSize, 300000);
                    if (docIds.isEmpty() || _reader.isEmpty())
                    {
                        System.out.println("Doc list empty. Breaking.");

                        break;
                    }

                    DbManager.bulkDelete(_bucket, docIds, 1000 * 60 * 15, 5);

                    System.out.println("Deleted:" + docIds.size());
                }
                catch (Exception ex)
                {
                    System.out.println("Exception. " + ex);
                }
            }

            _latch.countDown();
        }
    }
}
