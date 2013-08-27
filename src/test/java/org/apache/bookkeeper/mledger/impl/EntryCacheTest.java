package org.apache.bookkeeper.mledger.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class EntryCacheTest {
    @Test(timeOut = 5000)
    void testRead() throws Exception {
        LedgerHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = new EntryCacheManager(new ManagedLedgerFactoryConfig());
        EntryCache entryCache = cacheManager.getEntryCache("name");

        byte[] data = new byte[10];
        for (int i = 0; i < 10; i++) {
            entryCache.insert(new EntryImpl(0, i, data));
        }

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertEquals(entries.size(), 10);
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                Assert.fail("should not have failed");
            }
        });
        counter.await();

        // Verify no entries were read from bookkeeper
        verify(lh, never()).asyncReadEntries(anyLong(), anyLong(), any(ReadCallback.class), any());
    }

    @Test(timeOut = 5000)
    void testReadMissingBefore() throws Exception {
        LedgerHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = new EntryCacheManager(new ManagedLedgerFactoryConfig());
        EntryCache entryCache = cacheManager.getEntryCache("name");

        byte[] data = new byte[10];
        for (int i = 3; i < 10; i++) {
            entryCache.insert(new EntryImpl(0, i, data));
        }

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertEquals(entries.size(), 10);
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                Assert.fail("should not have failed");
            }
        });
        counter.await();
    }

    @Test(timeOut = 5000)
    void testReadMissingAfter() throws Exception {
        LedgerHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = new EntryCacheManager(new ManagedLedgerFactoryConfig());
        EntryCache entryCache = cacheManager.getEntryCache("name");

        byte[] data = new byte[10];
        for (int i = 0; i < 8; i++) {
            entryCache.insert(new EntryImpl(0, i, data));
        }

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertEquals(entries.size(), 10);
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                Assert.fail("should not have failed");
            }
        });
        counter.await();
    }

    @Test(timeOut = 5000)
    void testReadMissingMiddle() throws Exception {
        LedgerHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = new EntryCacheManager(new ManagedLedgerFactoryConfig());
        EntryCache entryCache = cacheManager.getEntryCache("name");

        byte[] data = new byte[10];
        entryCache.insert(new EntryImpl(0, 0, data));
        entryCache.insert(new EntryImpl(0, 1, data));
        entryCache.insert(new EntryImpl(0, 8, data));
        entryCache.insert(new EntryImpl(0, 9, data));

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertEquals(entries.size(), 10);
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                Assert.fail("should not have failed");
            }
        });
        counter.await();
    }

    @Test(timeOut = 5000)
    void testReadMissingMultiple() throws Exception {
        LedgerHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = new EntryCacheManager(new ManagedLedgerFactoryConfig());
        EntryCache entryCache = cacheManager.getEntryCache("name");

        byte[] data = new byte[10];
        entryCache.insert(new EntryImpl(0, 0, data));
        entryCache.insert(new EntryImpl(0, 2, data));
        entryCache.insert(new EntryImpl(0, 5, data));
        entryCache.insert(new EntryImpl(0, 8, data));

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertEquals(entries.size(), 10);
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                Assert.fail("should not have failed");
            }
        });
        counter.await();
    }

    @Test(timeOut = 5000)
    void testReadWithError() throws Exception {
        final LedgerHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        doAnswer(new Answer<Object>() {
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                ReadCallback callback = (ReadCallback) args[2];
                callback.readComplete(BKException.Code.NoSuchLedgerExistsException, lh, null, null);
                return null;
            }
        }).when(lh).asyncReadEntries(anyLong(), anyLong(), any(ReadCallback.class), any());

        EntryCacheManager cacheManager = new EntryCacheManager(new ManagedLedgerFactoryConfig());
        EntryCache entryCache = cacheManager.getEntryCache("name");

        byte[] data = new byte[10];
        entryCache.insert(new EntryImpl(0, 2, data));

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                Assert.fail("should not complete");
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }
        });
        counter.await();
    }

    private static LedgerHandle getLedgerHandle() {
        final LedgerHandle lh = mock(LedgerHandle.class);
        final LedgerEntry ledgerEntry = mock(LedgerEntry.class);
        doAnswer(new Answer<Object>() {
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                long firstEntry = (Long) args[0];
                long lastEntry = (Long) args[1];
                ReadCallback callback = (ReadCallback) args[2];

                Vector<LedgerEntry> entries = new Vector<LedgerEntry>();
                for (int i = 0; i <= (lastEntry - firstEntry); i++) {
                    entries.add(ledgerEntry);
                }
                callback.readComplete(0, lh, entries.elements(), callback);
                return null;
            }
        }).when(lh).asyncReadEntries(anyLong(), anyLong(), any(ReadCallback.class), any());

        return lh;
    }

}
