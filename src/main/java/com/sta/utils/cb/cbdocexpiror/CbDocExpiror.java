package com.sta.utils.cb.cbdocexpiror;

import com.couchbase.client.java.Bucket;
import com.sta.utils.dfs.FsUtils;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class CbDocExpiror
{

    private static DbManager _db;
    private final TimeRangeViewReaderDelete _reader;

    private final Bucket _cbBucket;

    public CbDocExpiror(TimeRangeViewReaderDelete reader, Bucket cbBucket)
    {
        _reader = reader;

        _cbBucket = cbBucket;
    }

    public static void main(String[] args) throws Exception
    {
        if ((args == null) || (args.length == 0) || (args[0] == null) || (args[0].isEmpty()))
        {
            System.out.println("This program needs atleast one argument. Config File Path is missing.");
        }

        JSONObject jsonConfig = FsUtils.readJsonFile(args[0]);
//        JSONObject jsonConfig = FsUtils.readJsonFile("D:\\adpushup\\CbDocExpiror.json");

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

        _db = new DbManager(cbIpArray, cbUser, cbPassword);

        Bucket cbBucket = _db.getBucket(cbBucketName);

        CbDocExpiror expire = new CbDocExpiror(new TimeRangeViewReaderDelete(cbBucket, cbDesignDocName, cbViewName, startKey, endKey), cbBucket);

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

        Calendar calTemp = Calendar.getInstance();
        calTemp.setTimeInMillis(startKey);
        System.out.println("From Date: " + calTemp.get(Calendar.YEAR) + "-" + (calTemp.get(Calendar.MONTH) + 1) + "-" + calTemp.get(Calendar.DATE) + ":" + calTemp.get(Calendar.HOUR_OF_DAY));

        calTemp = Calendar.getInstance();
        calTemp.setTimeInMillis(endKey);
        System.out.println("To Date: " + calTemp.get(Calendar.YEAR) + "-" + (calTemp.get(Calendar.MONTH) + 1) + "-" + calTemp.get(Calendar.DATE) + ":" + calTemp.get(Calendar.HOUR_OF_DAY));

        System.out.println("");
        System.out.println("********************************************************************************************");
        System.out.println("********************************************************************************************");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");

        System.out.println("Waiting for 10 sec before starting.");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        TimeUnit.SECONDS.sleep(10);

        System.out.println("**************************************Delete data START************************************************");

        expire.deleteDataAsync();

        System.out.println("**************************************Delete data FINISH************************************************");
    }

    private void deleteDataAsync() throws Exception
    {
        while (true)
        {
            try
            {
                List<String> docIds = _reader.getNext(5000, 30000);
                if (docIds.isEmpty())
                {
                    System.out.println("Doc list empty. Breaking.");
                    break;
                }

                DbManager.bulkDelete(_cbBucket, docIds, 1000 * 60 * 5, 1);

                System.out.println("Deleted:" + docIds.size());
            }
            catch (Exception ex)
            {
                System.out.println("Exception." + ex);
            }
        }
    }
}
