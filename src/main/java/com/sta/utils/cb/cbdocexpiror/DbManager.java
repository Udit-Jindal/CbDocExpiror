package com.sta.utils.cb.cbdocexpiror;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.util.retry.RetryBuilder;
import java.io.Closeable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import rx.Observable;
import rx.functions.Func1;

public class DbManager implements Closeable
{

    //variables
    final Cluster _cluster;

    Bucket _bucket;

    //constructor
    public DbManager(String[] couchbaseIPs, String username, String password)
    {
        _cluster = CouchbaseCluster.create(couchbaseIPs);
        _cluster.authenticate(username, password);
    }

    @Override
    protected void finalize() throws Throwable
    {
        try
        {
            _cluster.disconnect();
        }
        finally
        {
            super.finalize();
        }
    }

    @Override
    public void close()
    {
        _cluster.disconnect();
    }

    public Bucket getBucket(String bucketName)
    {
        if ((_bucket == null) || _bucket.isClosed())
        {
            synchronized (_cluster)
            {
                if ((_bucket == null) || _bucket.isClosed())
                {
                    _bucket = _cluster.openBucket(bucketName, 30, TimeUnit.SECONDS);
                }
            }
        }

        return _bucket;
    }

    public static void bulkDelete(final Bucket bucket, List<String> docIds, long timeout, int retries)
    {
        if (docIds.isEmpty())
        {
            return;
        }

        try
        {
            Observable
                    .from(docIds)
                    .flatMap(new Func1<String, Observable<JsonDocument>>()
                    {
                        @Override
                        public Observable<JsonDocument> call(String docId)
                        {
                            return bucket.async().remove(docId);
                        }
                    })
                    .retryWhen(
                            RetryBuilder.anyOf(BackpressureException.class)
                            .delay(Delay.exponential(TimeUnit.MILLISECONDS, timeout))
                            .max(retries)
                            .build()
                    )
                    .onErrorResumeNext(new Func1<Throwable, Observable<? extends JsonDocument>>()
                    {
                        @Override
                        public Observable<? extends JsonDocument> call(Throwable throwable)
                        {
                            if (throwable instanceof DocumentDoesNotExistException)
                            {
                                return Observable.empty();
                            }
                            else
                            {
                                return Observable.error(throwable);
                            }
                        }
                    })
                    .toBlocking()
                    .last();
        }
        catch (NoSuchElementException ex)
        {
            //no items returned
        }
    }

    public static void bulkDeleteDocs(final Bucket bucket, List<JsonDocument> docList, long timeout, int retries)
    {
        if (docList.isEmpty())
        {
            return;
        }

        try
        {
            Observable
                    .from(docList)
                    .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>()
                    {
                        @Override
                        public Observable<JsonDocument> call(JsonDocument doc)
                        {
                            return bucket.async().remove(doc.id());
                        }
                    })
                    .retryWhen(
                            RetryBuilder.anyOf(BackpressureException.class)
                            .delay(Delay.exponential(TimeUnit.MILLISECONDS, timeout))
                            .max(retries)
                            .build()
                    )
                    .onErrorResumeNext(new Func1<Throwable, Observable<? extends JsonDocument>>()
                    {
                        @Override
                        public Observable<? extends JsonDocument> call(Throwable throwable)
                        {
                            if (throwable instanceof DocumentDoesNotExistException)
                            {
                                return Observable.empty();
                            }
                            else
                            {
                                return Observable.error(throwable);
                            }
                        }
                    })
                    .toBlocking()
                    .last();
        }
        catch (NoSuchElementException ex)
        {
            //no items returned
        }
    }
}
