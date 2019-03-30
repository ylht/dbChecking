# dbChecking

## 事务定义

### 基础事务（独立于隔离级别）

1. 转账事务

   + 定义

     事务在一组列内操作，首先在其中选取一列，从该列中的一项中减去一个数，再将这个数加到另一个随机选择的列的随机一项上，随机的列可以是本列。

     验证方式为，需要确保所有列的起始和和终止和一致。

   + 事务模版

   ```sql
   update TABLE_1 set column_a = column_a - <value> 
   where key=x and coulumn_a > <value>
   
   update TABLE_2 set column_b = column_b + <value> where key=y
   ```

   + 正确性验证公式

   $$
   sum(column\_a)_{pre}+sum(column\_b)_{pre}\\
   =sum(column\_a)_{post}+sum(column\_b)_{post}
   $$

2. 比例型事务

   + 定义：

     事务在两个列上进行操作，对这两个列同时做加，每次需要保证在第二个列上加的值是一个列的K倍。

     验证方式为，需要确保第二列的终止和减去起始和为第一列的终止和减去起始和的K倍。

   + 事务模版

   ```sql
   update TABLE_1 set column_a= column_a + <value> where key=x
   update TABLE_2 set column_b = column_b + k * <value> where key=y 
   ```

   + 正确性验证公式

   $$
   𝑠𝑢𝑚(𝑐𝑜𝑙𝑢𝑚𝑛\_𝑏)_{𝑝𝑜𝑠𝑡}−𝑠𝑢𝑚(𝑐𝑜𝑙𝑢𝑚𝑛\_𝑏)_{𝑝𝑟𝑒}\\=𝑘∗(𝑠𝑢𝑚(𝑐𝑜𝑙𝑢𝑚𝑛\_𝑎)_{𝑝𝑜𝑠𝑡}−𝑠𝑢𝑚(𝑐𝑜𝑙𝑢𝑚𝑛\_𝑎)_{𝑝𝑟𝑒})
   $$

   

3. 订单事务

   + 定义：

     事务在单列上操作，每次选取该列的一个元组对其减1，然后在ITEM表中插入一个记录项。

     验证方式为，需要保证ITEM表中的记录项数量之和等于该列的起始和减去终止和。

   + 事务模版

     ```sql
     update TABLE_1 set column_a = column_a -1 where key=x
     Insert into ITEM values(<TABLE_1>,<column_a>)
     ```

   + 正确性验证公式
     $$
     𝑠𝑢𝑚(𝑐𝑜𝑙𝑢𝑚𝑛\_𝑎)_{𝑝𝑟𝑒}−𝑠𝑢𝑚(𝑐𝑜𝑙𝑢𝑚𝑛\_𝑎)_{𝑝𝑜𝑠𝑡}=\\
     𝑐𝑜𝑢𝑛𝑡(𝐼𝑇𝐸𝑀.𝑡𝑎𝑏𝑙𝑒𝑁𝑎𝑚𝑒=𝑇𝐴𝐵𝐿𝐸\_1,𝐼𝑇𝐸𝑀.𝑐𝑜𝑙𝑢𝑚𝑛𝑁𝑎𝑚𝑒=𝑐𝑜𝑙𝑢𝑚𝑛\_𝑎)
     $$
     

### Read Uncommitted

采用基础事务即可，验证是否存在脏写。

### Read Committed

1. 脏写

   采用基础事务即可

2. 脏读

   + 事务定义

     采用两组事务

     1. 事务1更新一个列的部分值为负数，然后sleep一段时间，之后回滚。
     2. 事务2从本列上select一个值，然后将该值覆盖的写到与之对应的记录项上，如果该记录项上已经有一个负数值时，则不写。

     验证方式为，需要确保所有的记录项上不出现负值。

   + 事务模版

     1. 更新事务

      ```sql
      update TABLE_1 set column_a =-1 where key between x and y
      Threads sleep a time
      rollback
      ```

     2. 校验事务

     ```sql
     select column_a from TABLE_1 where key = z
     update TABLE_1 set Record = <column_a> where key = z and Record>=0;
     ```

   + 正确性验证公式

     $$
     count(TABLE\_1.Record<0)=0
     $$

### Repeatable Read

1. 脏写

   采用基础事务即可

2. 脏读

   事务和检验方式同Read Committed

3. 模糊读/不可重复读

   + 事务定义

     对一个列执行select语句，之后sleep一段时间，再次对该列做select，将两次的差值加到记录项上。

     验证方式为，记录项所有的值应该全部等于0。

   + 事务模板

     ```sql
     select column_a from TABLE_1  where key=x
     Thread sleep a time
     select column_a from TABLE_1  where key=x
     update TABLE_1 set Diff_Record = Diff_Record + diff<column_a> where key = x
     ```

   + 正确性验证公式
     $$
     𝑐𝑜𝑢𝑛𝑡(Diff\_Record!=0)=0
     $$
     

### Serialiable

1. 脏写，幻读

   将基础事务的执行语句改为先select后update的方案（待证明），或者可以采用之前的阐述的事务来检测，如果可以证明，便可以将事务量简化，同时似乎丢失更新和读偏也可以用该事务一同验证，不过仍是待证明。

2. 脏读

   采用Read Committed中的验证方案

3. 幻读

   + 事务定义

     需要检验两种类型的幻读，分别是元组增删和元组修改带来的幻读。

     + 对于元组增删带来的幻读，采用两组事务。

     1. 事务1在一张独立的表上做增删。

     2. 事务2首先根据一列的range获取一组主键值，之后sleep一段时间，然后update盖range内的所有值加1，再次执行相同的语句，但是此时的range中min和max相应的都要加1，以保证理论上获取的值是相同的，最后回滚撤销修改。用此种方式可以检测MySql中具有欺骗性的幻读出现情景。

        在事务2结束之后，比较两次获取的主键是不是完全一样，如果不一样在幻读记录中插入一条记录，一样则不执行任何操作。

     + 对于元组删除带来的幻读，只需要执行事务2即可

     验证方式为，需要保证幻读记录表中不存在任何记录。

   + 事务模板

     1. 事务1

     ```sql
     replace into TABLE_1 values(?...?)
     ```

     2. 事务2

     ```sql
     select key from TABLE_1 where column_a between x and y
     Thread sleep a time
     update TABLE_1 set column_a = column_a +1 where column_a between x and y
     select key from TABLE_1 where column_a between x+1 and y+1
     rollback
     ```

     3. 插入事务

     ```sql
     insert into Phantom_Read values (<Table_1>,Error_Type)
     ```

   + 正确性验证公式
     $$
     count(Phantom\_Read)=0
     $$
     

4. 写偏

   + 事务定义

     读取两个列的同一行值，对一个列的值做差，减去他们和的1/2以上的值。

     验证方式为，需要保证最终验证时，两列的和是大于等于0的。

   + 事务模版

     ```sql
     select column_a from TABLE_1 where key=x
     select column_b from TABLE_1 where key=x
     update TABLE_1 set column_a= column_a-(column_a+column_b)*4/5 where key =x
     ```

   + 正确性验证公式
     $$
     count((column\_a+coulumn\_b)<0)=0
     $$
     