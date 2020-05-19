#
# This is a simple makefile to assist with quickly building the Exercises of MP2.
#
# To build and execute a particular exercise:
#    - For a single exercise, type 'make runA' to run exercise A.
#    - For all exercises, 'make'.
#
#
HADOOP_CLASSPATH := ${JAVA_HOME}/lib/tools.jar
export HADOOP_CLASSPATH

HDFS=user/lada04/bigdata

OBJDIR=build

JAR := MapReducePSO.jar

TARGETS := $(addprefix run, A B C D E F)

.PHONY: final $(TARGETS) clean

final: $(TARGETS)

runA: $(OBJDIR)/VisitorFilter.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/1-output/
	hadoop jar $(JAR) VisitorFilter -D N=5  /user/adampap/WHVsmall /$(HDFS)/1-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/1-output/part*"

runB: $(OBJDIR)/VisitorSearcher.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/2-output/
	hadoop jar $(JAR) VisitorSearcher -D N=10  /user/adampap/WHV /$(HDFS)/2-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/2-output/part*"

runC: $(OBJDIR)/VisitorCalendar.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/3-output/
	hadoop jar $(JAR) VisitorCalendar /user/adampap/WHV /$(HDFS)/3-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/3-output/part*"

runD: $(OBJDIR)/VisitorHour.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/4-output/
	hadoop jar $(JAR) VisitorHour /user/adampap/WHV /$(HDFS)/4-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/4-output/part*"

runE: $(OBJDIR)/TopPopularLinks.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/E-output/
	hadoop jar $(JAR) TopPopularLinks -D N=5 /user/adampap/psso/links /$(HDFS)/E-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/E-output/part*"

runF: $(OBJDIR)/PopularityLeague.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/F-output/
	hadoop jar $(JAR) PopularityLeague -D league=/$(HDFS)/misc/league.txt /user/adampap/psso/links /$(HDFS)/F-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/F-output/part*"


$(OBJDIR)/%.class: %.java | $(OBJDIR)
	hadoop com.sun.tools.javac.Main $< -d $(OBJDIR)

$(OBJDIR):
	mkdir $@

.PHONY: clean
clean:
	rm -f $(OBJDIR)/* $(JAR)
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/*-output/
