package planbuilder2

type planType int

const (
	NONE planType = iota
	ROUTE
	FILTER
	PROJECT
)

func optimize(plan logicalPlan) (logicalPlan, bool) {
	optimized := false

	var (
		this, left  planType
		thisProject *project
		thisFilter  *filter
		leftRoute   *route
	)
	for {
		switch p := plan.(type) {
		case *project:
			this = PROJECT
			thisProject = p
		case *filter:
			this = FILTER
			thisFilter = p
		case *route:
			this = ROUTE
		}

		inputs := plan.Inputs()
		if len(inputs) == 1 {
			l := inputs[0]
			l, wasOptimized := optimize(l)
			if wasOptimized {
				// we had one input to start with, so we know we can do this next step
				plan.Rewrite1(l)
			}
			switch p := l.(type) {
			case *project:
				left = PROJECT
			case *filter:
				left = FILTER
			case *route:
				left = ROUTE
				leftRoute = p
			}
		}

		// we continue looping until we can't find anything to improve
		switch {
		case this == PROJECT && left == ROUTE:
			plan = leftRoute
			leftRoute.query.SelectExprs = append(leftRoute.query.SelectExprs, thisProject.columns...)
		case this == FILTER && left == ROUTE:
			plan = leftRoute
			leftRoute.query.AddWhere(thisFilter.predicate)
		default:
			return plan, optimized
		}
		optimized = true
	}
}
