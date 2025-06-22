package org.apdplat.data.generator;

import org.apdplat.data.generator.mysql.MySQLUtils;
import org.apdplat.data.generator.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
public class DataGeneratorApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataGeneratorApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(DataGeneratorApplication.class, args);
    }

    @PostConstruct
    public void run() {
        LOGGER.info("开始生成数据");
        int batchSize = Config.getIntValue("batchSize") == -1 ? 1000 : Config.getIntValue("batchSize");
        //客户数
        int customerCount = Config.getIntValue("customerCount") == -1 ? 5000 : Config.getIntValue("customerCount");
        //销售数
        int salesStaffCount = Config.getIntValue("salesStaffCount") == -1 ? 2000 : Config.getIntValue("salesStaffCount");
        //商品数
        int itemCount = Config.getIntValue("itemCount") == -1 ? 10000 : Config.getIntValue("itemCount");
        //商品价格上限
        int priceLimit = Config.getIntValue("priceLimit") == -1 ? 1000 : Config.getIntValue("priceLimit");
        //商品类别数
        int categoryCount = Config.getIntValue("itemCount") == -1 ? 10000 : Config.getIntValue("itemCount");
        //商品品牌数
        int brandCount = MySQLUtils.getCount("brand");
        //合同最大明细数
        int contractDetailLimit = Config.getIntValue("contractDetailLimit") == -1 ? 100 : Config.getIntValue("contractDetailLimit");
        //合同明细商品最大数量
        int itemQuantityLimit = Config.getIntValue("itemQuantityLimit") == -1 ? 100 : Config.getIntValue("itemQuantityLimit");

        // 一天是多少秒
        int secondsPerDay = 24 * 60 * 60;
        // 每天新增订单数
        int dailyOrderCount = Config.getIntValue("dailyOrderCount") == -1 ?
                10000000 : Config.getIntValue("dailyOrderCount");
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (true) {
            try {
                Thread.sleep(1000);
                // 起始年月日为当前日期
                int startYear = LocalDate.now().getYear();
                int startMonth = LocalDate.now().getMonthValue();
                int startDay = LocalDate.now().getDayOfMonth();
                String currentDate = String.format("%04d-%02d-%02d 00:00:00", startYear, startMonth, startDay);
                // 每秒新增订单数
                int ordersPerSecond = random.nextInt(((dailyOrderCount / secondsPerDay) + 1) * 2);
                LOGGER.info("当前时间：{}，每秒新增订单数：{}", currentDate, ordersPerSecond);
                generateContract(ordersPerSecond, contractDetailLimit, itemCount, itemQuantityLimit, currentDate, customerCount, salesStaffCount, batchSize);
                LOGGER.info("当前时间：{}，新增订单完成", currentDate);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void generateContract(int ordersPerSecond, int contractDetailLimit, int itemCount, int itemQuantityLimit,
                                  String currentDate, int customerCount, int salesStaffCount, int batchSize) {
        Connection con = null;
        PreparedStatement contractPst = null;
        PreparedStatement detailPst = null;
        PreparedStatement itemPst = null;
        ResultSet rs = null;
        List<String> states = Arrays.asList("新建", "签订", "生效", "履行中", "终止", "作废");
        Random random = new Random();

        try {
            con = MySQLUtils.getConnection();
            if (con == null) {
                return;
            }
            con.setAutoCommit(false);

            // 合同插入语句
            String contractSql = "insert into contract (contract_price, state, sign_day, sales_staff_id, customer_id) values(?, ?, ?, ?, ?)";
            contractPst = con.prepareStatement(contractSql, PreparedStatement.RETURN_GENERATED_KEYS);

            // 合同详情插入语句
            String detailSql = "insert into contract_detail (item_id, item_quantity, detail_price, contract_id, sign_day) values(?, ?, ?, ?, ?)";
            detailPst = con.prepareStatement(detailSql);

            // 查询商品信息
            String itemSql = "SELECT id, price FROM item where id = ?";
            itemPst = con.prepareStatement(itemSql);

            for (int i = 0; i < ordersPerSecond; i++) {
                int salesStaffId = random.nextInt(salesStaffCount) + 1;
                int customerId = random.nextInt(customerCount) + 1;
                String state = states.get(random.nextInt(states.size()));
                float totalPrice = 0;

                int itemNum = random.nextInt(contractDetailLimit) + 1;
                Set<Integer> itemUsed = new HashSet<>();
                List<ContractDetail> details = new ArrayList<>(); // 存储合同详情信息

                for (int j = 0; j < itemNum; j++) {
                    int itemId = random.nextInt(itemCount) + 1;
                    while (itemUsed.contains(itemId)) {
                        itemId = random.nextInt(itemCount) + 1;
                    }
                    itemUsed.add(itemId);

                    itemPst.setInt(1, itemId);
                    rs = itemPst.executeQuery();
                    float price = 0;
                    if (rs.next()) {
                        price = rs.getFloat("price");
                    }
                    int itemQuantity = random.nextInt(itemQuantityLimit) + 1;
                    float detailPrice = price * itemQuantity;
                    totalPrice += detailPrice;

                    // 存储合同详情信息
                    details.add(new ContractDetail(itemId, itemQuantity, detailPrice));
                }

                // 插入合同
                contractPst.setFloat(1, totalPrice);
                contractPst.setString(2, state);
                contractPst.setString(3, currentDate);
                contractPst.setInt(4, salesStaffId);
                contractPst.setInt(5, customerId);
                contractPst.addBatch();

                // 执行合同插入并获取生成的合同 ID
                contractPst.executeBatch();
                rs = contractPst.getGeneratedKeys();
                int contractId = 0;
                if (rs.next()) {
                    contractId = rs.getInt(1);
                }

                // 插入合同详情
                for (ContractDetail detail : details) {
                    detailPst.setInt(1, detail.getItemId());
                    detailPst.setInt(2, detail.getItemQuantity());
                    detailPst.setFloat(3, detail.getDetailPrice());
                    detailPst.setInt(4, contractId);
                    detailPst.setString(5, currentDate);
                    detailPst.addBatch();
                }
            }

            // 执行合同详情批处理
            detailPst.executeBatch();
            con.commit();
        } catch (SQLException e) {
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException rollbackEx) {
                LOGGER.error("回滚失败", rollbackEx);
            }
            LOGGER.error("插入合同和合同详情失败", e);
        } finally {
            MySQLUtils.close(con, contractPst, rs);
            MySQLUtils.close(null, detailPst, null);
            MySQLUtils.close(null, itemPst, null);
        }
    }

    // 合同详情信息类
    private static class ContractDetail {
        private final int itemId;
        private final int itemQuantity;
        private final float detailPrice;

        public ContractDetail(int itemId, int itemQuantity, float detailPrice) {
            this.itemId = itemId;
            this.itemQuantity = itemQuantity;
            this.detailPrice = detailPrice;
        }

        public int getItemId() {
            return itemId;
        }

        public int getItemQuantity() {
            return itemQuantity;
        }

        public float getDetailPrice() {
            return detailPrice;
        }
    }
}
