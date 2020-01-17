package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Test {
    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.101.71.42");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static boolean isTableExist(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }

    public static void createTable(String tableName, String... columnFamilies) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);

        if (!isTableExist(tableName)) {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));

            for (String cf : columnFamilies) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }

            admin.createTable(descriptor);
        }
    }

    public static void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (isTableExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        HTable hTable = new HTable(conf, tableName);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));

        hTable.put(put);
        hTable.close();
    }

    public static void deleteRows(String tableName, String... rows) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        List<Delete> deletes = new ArrayList<Delete>();

        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deletes.add(delete);
        }

        hTable.delete(deletes);
        hTable.close();
    }

    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();

        ResultScanner results = hTable.getScanner(scan);

        showResults(results);
    }

    public static void getRow(String tableName, String rowKey) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = hTable.get(get);

        for (Cell cell : result.rawCells()) {
            String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            Long timeStamp = cell.getTimestamp();

            System.out.println("rowKey: " + rowKey + ", columnFamily: " + columnFamily + ", column: " + column + ", value: " + value);
        }
    }

    public static void getRowQualifier(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));

        Result result = hTable.get(get);
        for (Cell cell: result.rawCells()) {
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("rowKey: " + rowKey + ", columnFamily: " + columnFamily + ", column: " + column + ", value: " + value);
        }
    }

    public static void getWithValueFilter(String tableName, String columnFamily, String column, String filterValue) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        //值过滤器
        Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(filterValue));//模糊匹配

        scan.setFilter(filter);
        ResultScanner results = hTable.getScanner(scan);

        showResults(results);
    }

    public static void getWithColumnValueFilter(String tableName, String columnFamily, String column, String filterValue) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        //单列值过滤器
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(filterValue)));//精准匹配

        scan.setFilter(filter);
        ResultScanner results = hTable.getScanner(scan);

        showResults(results);
    }

    public static void getWithLimitFilter(String tableName, int limit) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        //分页过滤器
        Filter filter = new PageFilter(limit);

        scan.setFilter(filter);
        ResultScanner results = hTable.getScanner(scan);

        showResults(results);
    }

    public static void getWithFilters(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();

        List<Filter> filters = new ArrayList<Filter>();

        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,new SubstringComparator("J"));
        Filter filter2 = new PageFilter(2);

        //注意：过滤器列表的先后顺序是会对结果造成影响的。如果把pageFilter放在前面，结果是不一样的
        filters.add(filter1);
        filters.add(filter2);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);//MUST_PASS_ALL\MUST_PASS_ONE
        scan.setFilter(filterList);

        ResultScanner results = hTable.getScanner(scan);

        showResults(results);
    }

    public static void getWithRowFilters(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();

//        Filter filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes("1003")));
//        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("4"));

        //多行范围过滤器
        // 构造RowRange 四个参数分别为startRow startRowInclusive(是否包含起始行) stopRow stopRowInclusive(是否包含结束行)
//        MultiRowRangeFilter.RowRange rowRange1 = new MultiRowRangeFilter.RowRange("1001",true,"1003",false);
//        MultiRowRangeFilter.RowRange rowRange2 = new MultiRowRangeFilter.RowRange("1005",false,"1007",true);
//        List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<MultiRowRangeFilter.RowRange>();
//        rowRanges.add(rowRange1);
//        rowRanges.add(rowRange2);
//        Filter filter = new MultiRowRangeFilter(rowRanges);

        //行键前缀过滤器
//        PrefixFilter filter = new PrefixFilter(Bytes.toBytes("100"));

        //模糊行键匹配
//        List<Pair<byte[], byte[]>> fuzzyKeysData = new ArrayList<Pair<byte[], byte[]>>();
//        Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>(
//                Bytes.toBytes("1005?"),
//                new byte[]{0,0,0,0,1}
//        );
//        fuzzyKeysData.add(pair);
//        FuzzyRowFilter filter = new FuzzyRowFilter(fuzzyKeysData);

        //包含结尾的过滤器
        //Scan scan = new Scan(Bytes.toBytes("10001"), Bytes.toBytes("10005"));//左开右闭

        //随机行过滤器
        Filter filter = new RandomRowFilter(new Float(0.5));//数据分析抽样采样


        scan.setFilter(filter);
        ResultScanner results = hTable.getScanner(scan);
        showResults(results);
    }

    public static void getWithColumnFilters(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();

        //列族过滤器
//        Filter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("info")));

        //列过滤器
//        Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("name")));

        //列前缀过滤器
//        Filter filter = new ColumnPrefixFilter(Bytes.toBytes("s"));

        //多列前缀过滤器
//        byte[][] prefixes = new byte[2][];
//        prefixes[0]=Bytes.toBytes("s");
//        prefixes[1]=Bytes.toBytes("a");
//        MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(prefixes);

        //列名过滤器
//        KeyOnlyFilter filter = new KeyOnlyFilter();//只需要筛选出列名，而不需要值

        //列名范围过滤器
        ColumnRangeFilter filter = new ColumnRangeFilter(
                Bytes.toBytes("age"), true, Bytes.toBytes("name"), true);//true false代表是否包含最大最小列


        scan.setFilter(filter);
        ResultScanner results = hTable.getScanner(scan);
        showResults(results);
    }

    public static void showResults(ResultScanner results) {
        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println("rowKey: " + rowKey + ", columnFamily: " + columnFamily + ", column: " + column + ", value: " + value);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        getWithColumnFilters("student");
    }
}
