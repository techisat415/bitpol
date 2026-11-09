package typologies

import "sort"

// JohnsonCycles returns every unique elementary cycle in a directed graph.
// The implementation restricts each search to the least vertex in its
// strongly connected component, giving Johnson's O((V+E)(c+1)) bound.
func JohnsonCycles(input map[string][]string) [][]string {
	graph := normalizeAdjacency(input)
	var cycles [][]string
	for len(graph) > 0 {
		sccs := stronglyConnected(graph)
		if len(sccs) == 0 {
			break
		}
		component := smallestCyclicComponent(sccs, graph)
		if len(component) == 0 {
			break
		}
		start := component[0]
		allowed := make(map[string]bool, len(component))
		for _, node := range component {
			allowed[node] = true
		}
		blocked := make(map[string]bool, len(component))
		blockedBy := make(map[string]map[string]bool, len(component))
		for _, node := range component {
			blockedBy[node] = make(map[string]bool)
		}
		stack := make([]string, 0, len(component))
		var circuit func(string) bool
		var unblock func(string)
		unblock = func(node string) {
			blocked[node] = false
			for next := range blockedBy[node] {
				delete(blockedBy[node], next)
				if blocked[next] {
					unblock(next)
				}
			}
		}
		circuit = func(node string) bool {
			found := false
			stack = append(stack, node)
			blocked[node] = true
			for _, next := range graph[node] {
				if !allowed[next] {
					continue
				}
				if next == start {
					cycle := append([]string(nil), stack...)
					cycles = append(cycles, cycle)
					found = true
				} else if !blocked[next] && circuit(next) {
					found = true
				}
			}
			if found {
				unblock(node)
			} else {
				for _, next := range graph[node] {
					if allowed[next] {
						blockedBy[next][node] = true
					}
				}
			}
			stack = stack[:len(stack)-1]
			return found
		}
		circuit(start)
		delete(graph, start)
		for node, neighbors := range graph {
			filtered := neighbors[:0]
			for _, next := range neighbors {
				if next != start {
					filtered = append(filtered, next)
				}
			}
			graph[node] = filtered
		}
	}
	return cycles
}

func normalizeAdjacency(input map[string][]string) map[string][]string {
	graph := make(map[string][]string, len(input))
	for node, neighbors := range input {
		graph[node] = nil
		for _, next := range neighbors {
			if contains(graph[node], next) {
				continue
			}
			graph[node] = append(graph[node], next)
			if _, ok := graph[next]; !ok {
				graph[next] = nil
			}
		}
		sort.Strings(graph[node])
	}
	return graph
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func stronglyConnected(graph map[string][]string) [][]string {
	index, low := 0, make(map[string]int)
	stack := make([]string, 0, len(graph))
	onStack := make(map[string]bool, len(graph))
	var components [][]string
	var visit func(string)
	visit = func(node string) {
		index++
		indexValue := index
		low[node] = indexValue
		stack = append(stack, node)
		onStack[node] = true
		for _, next := range graph[node] {
			if _, seen := low[next]; !seen {
				visit(next)
				if low[next] < low[node] {
					low[node] = low[next]
				}
			} else if onStack[next] && low[next] < low[node] {
				low[node] = low[next]
			}
		}
		if low[node] == indexValue {
			var component []string
			for {
				last := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[last] = false
				component = append(component, last)
				if last == node {
					break
				}
			}
			sort.Strings(component)
			components = append(components, component)
		}
	}
	nodes := make([]string, 0, len(graph))
	for node := range graph {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		if _, seen := low[node]; !seen {
			visit(node)
		}
	}
	return components
}

func smallestCyclicComponent(components [][]string, graph map[string][]string) []string {
	for _, component := range components {
		if len(component) > 1 {
			return component
		}
		node := component[0]
		for _, next := range graph[node] {
			if next == node {
				return component
			}
		}
	}
	return nil
}
