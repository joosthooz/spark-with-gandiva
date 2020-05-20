package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSessionExtensions, Strategy}


object ProjectionOnGandivaExtension {
  def apply(): (SparkSessionExtensions => Unit) = { e: SparkSessionExtensions =>
    e.injectPlannerStrategy(_ => new ProjectionOnGandivaStrategy())
  }
}

class ProjectionOnGandivaStrategy() extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case logical.Project(projectList, child) => Seq(GandivaProjectExec(planLater(child), projectList))
    case logical.Filter(condition, child) => Seq(GandivaFilterExec(planLater(child), condition))
    case _ => Nil
  }
}