
from base import BaseFilter
import pyspark.sql.functions as F


class TopRepoFilter(BaseFilter):
    def filter(self, df):
        # Top 10 Repo: Push Count
        repo_cnt_df = df.groupBy('repository_id', 'repo_name').pivot('type').count()
        repo_cnt_df.where((F.col('repository_id').isNotNull())) \
                    .orderBy(F.desc('PushEvent')) \
                    .limit(10)
        return repo_cnt_df

class TopUserFilter(BaseFilter):
    def filter(self, df):
        # Top 10 User: Push Count
        user_cnt_df = df.groupBy('user_name').pivot('type').count()
        user_cnt_df.where((~F.col('user_name').contains('[bot]'))) \
                    .orderBy(F.desc('PushEvent')) \
                    .limit(10)
        return user_cnt_df
        
class DailyStatFilter(BaseFilter):
    def hit_count(self, df, cond, col_name):
        return df.withColumn('is_cond', F.when(cond, 1).otherwise(0)).agg(F.sum('is_cond').alias(col_name))

    def filter(self, df):
        # daily stats
        stat_df = df.agg(F.countDistinct('user_name').alias('d_user_count'))
        stat_df = stat_df.crossJoin(df.agg(F.countDistinct('repository_id').alias('d_repo_count')))

        push_cnt_df = self.hit_count(df, F.col('type') == 'PushEvent', 'push_count')
        push_cnt_df = push_cnt_df.cache()
        stat_df = stat_df.crossJoin(push_cnt_df)

        pr_cnt_df = self.hit_count(df, F.col('type') == 'PullRequestEvent', 'pr_count')
        pr_cnt_df = pr_cnt_df.cache()
        stat_df = stat_df.crossJoin(pr_cnt_df)

        fork_cnt_df = self.hit_count(df, F.col('type') == 'ForkEvent', 'fork_count')
        fork_cnt_df = fork_cnt_df.cache()
        stat_df = stat_df.crossJoin(fork_cnt_df)

        commit_comment_cnt_df = self.hit_count(df, F.col('type') == 'CommitCommentEvent', 'commit_comment_count')
        commit_comment_cnt_df = commit_comment_cnt_df.cache()
        stat_df = stat_df.crossJoin(commit_comment_cnt_df)

        stat_df.show(10, False)
        return stat_df

class MonthlyStatFilter(BaseFilter):
    def read_input(self, target_date):
        # target_date: yyyy-MM
        return self.spark.read.json(f"/opt/bitnami/spark/data/{target_date}-*.json")

    def filter(self, df):
        # daily stats
        stat_df = df.agg(F.countDistinct('user_name').alias('d_user_count'))
        stat_df = stat_df.crossJoin(df.agg(F.countDistinct('repository_id').alias('d_repo_count')))

        return stat_df
    
class PytorchTopIssuerFilter(BaseFilter):
    def filter(self, df):
        # Filter : repo_name = pytorch
        base_df = df.filter(F.col('userid_and_repo_name') == 'pytorch/pytorch')

        # groupby : 
        result_df = base_df.groupBy('user_name').pivot('type').count()
        result_df = result_df.cache()
        result_df.where((~F.col('user_name').contains('[bot]'))) \
                    .orderBy(F.desc('IssuesEvent')) \
                    .limit(10)
        return result_df
