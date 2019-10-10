package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            // TODO(hw3_part1): implement
            //sort left & right
            String leftSortedTableName = new SortOperator(getTransaction(),getLeftTableName(),new LeftRecordComparator()).sort();
            String rightSortedTableName = new SortOperator(getTransaction(),getRightTableName(),new RightRecordComparator()).sort();
            leftIterator = SortMergeOperator.this.getRecordIterator(leftSortedTableName);
            rightIterator = SortMergeOperator.this.getRecordIterator(rightSortedTableName);
            leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            //copied from SNLJOperator.java
            // We mark the first record so we can reset to it when we advance the left record.
            if (rightRecord != null) {
                rightIterator.markPrev();
            } else { return; }
            try {
               fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(hw3_part1): implement
            //copied from SNLJOperator.java
            return this.nextRecord != null;
        }

        private void resetRightRecord() {
            //copied from SNLJOperator.java
            this.rightIterator.reset();
            assert(rightIterator.hasNext());
            rightRecord = rightIterator.next();
        }

        private int compareLeftToRightRecord() {
            DataBox leftJoinValue = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
            DataBox rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
            return leftJoinValue.compareTo(rightJoinValue);
        }

        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        private void leftRecordNullCheck() {
            if (leftRecord == null) { throw new NoSuchElementException("No new record to fetch"); }
        }
        private void advanceLeftRecord() {
            leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            leftRecordNullCheck();
        }

        private void advanceRightRecord() {
            rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
        }

        private void fetchNextRecord() {
            //copied some skeleton logic from SNLJOperator.java
            leftRecordNullCheck();
            nextRecord = null;
            do {
                if (rightRecord != null && !marked) {
                    while (compareLeftToRightRecord() < 0) { //left < right
                        advanceLeftRecord();
                    }
                    while (compareLeftToRightRecord() > 0) { //left > right
                        advanceRightRecord();
                    }
                    marked = true;
                    rightIterator.markPrev();
                }
                if (rightRecord != null && compareLeftToRightRecord() == 0) { //left == right
                    nextRecord = joinRecords(leftRecord,rightRecord);
                    advanceRightRecord();
                }
                else {
                    resetRightRecord();
                    advanceLeftRecord();
                    marked = false;
                }
            } while (!hasNext());
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(hw3_part1): implement
            //copied from SNLJOperator.java
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
