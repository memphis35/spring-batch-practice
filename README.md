# Spring Batch practice repository

a course by _Kiryl Yesipau_ is [available here](https://www.udemy.com/course/spring-batch-mastery)

### Technology stack

 - Java 21
 - Gradle 8.8
 - Spring Boot 3.3.3
 - Spring Batch 5.1.2 (provided by spring-boot-starter-batch dependency)
 
### Beginner task

Temperature sensors daily report, [source](/src/main/java/com/example/batch_jobs/sensors)

 - read sensor's data from a file
 - aggregate measures and analyze
 - write XML report with abnormal measures

### Intermediate task

Team's performance analysis [source](/src/main/java/com/example/batch_jobs/teams)

 - read unconventionally formatted data from several files
 - analyze team scores, calculate aggregated values
 - write two unconventionally formatted reports

### Advanced task

Account balance calculations, [source](/src/main/java/com/example/batch_jobs/transactions)

 - read bank transactions from DB
 - calculate a balance sequentially, aggregate results
 - write a JSON report based on the balance

### Expert task

Players score calculation [source](/src/main/java/com/example/batch_jobs/coins)

 - read all scores from DB
 - calculate total score based on score type (multiplication or addition)
 - insert/update player's total