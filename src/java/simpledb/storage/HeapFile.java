package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageNumber = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "r");
            raf.seek((long) pageNumber * pageSize);
            byte[] data = new byte[pageSize];
            for (int i = 0; i < pageSize; i++) {
                data[i] = raf.readByte();
            }
            raf.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (Exception e) {
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId id = page.getId();
        int tableId = id.getTableId();
        int pn = id.getPageNumber();

        HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        RandomAccessFile rws = new RandomAccessFile(hf.getFile(), "rws");
        rws.skipBytes(pn * BufferPool.getPageSize());
        rws.write(page.getPageData());
        rws.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }


    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        int pgNo = 0;
        HeapPage page = null;
        List<Page> pages = new ArrayList<>();
        for (; pgNo < numPages(); pgNo++) {
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                pages.add(page);
                return pages;
            }
        }
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file, true));
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        bos.write(emptyPageData);
        bos.close();

        HeapPageId heapPageId = new HeapPageId(getId(), pgNo);
        page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
        page.insertTuple(t);

        pages.add(page);
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<Page>() {{
            add(page);
        }};
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int nextPage = 0;
            private Iterator<Tuple> tupleItr;

            private boolean isOpen;

            private HeapPage getPageFromPool() throws TransactionAbortedException, DbException {
                return (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), nextPage++), Permissions.READ_ONLY);
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                tupleItr = getPageFromPool().iterator();
                isOpen = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!isOpen) return false;
                while (true) {
                    if (tupleItr.hasNext()) return true;
                    else if (nextPage < numPages()) {
                        tupleItr = getPageFromPool().iterator();
                    } else
                        break;
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!isOpen) throw new NoSuchElementException();
                return tupleItr.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (!isOpen) return;
                nextPage = 0;
                tupleItr = getPageFromPool().iterator();
            }

            @Override
            public void close() {
                if (!isOpen) return;
                isOpen = false;
                tupleItr = null;
            }
        };
    }

}

