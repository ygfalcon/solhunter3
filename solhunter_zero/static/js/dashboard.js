(function () {
    const dataElement = document.getElementById("dashboard-data");
    if (!dataElement) {
        return;
    }

    let payload = {};
    try {
        const raw = dataElement.textContent || "";
        payload = raw ? JSON.parse(raw) : {};
    } catch (error) {
        // eslint-disable-next-line no-console
        console.error("Failed to parse dashboard data", error);
        return;
    }

    const history = Array.isArray(payload.history) ? payload.history : [];
    const weightLabels = Array.isArray(payload.weights_labels)
        ? payload.weights_labels
        : [];
    const weightValues = Array.isArray(payload.weights_values)
        ? payload.weights_values
        : [];

    const hasChart = typeof window !== "undefined" && window.Chart;

    if (hasChart && history.length) {
        const labels = history.map((h) => (h.timestamp || "").slice(11, 19));
        const actionsData = history.map((h) => h.actions_count || 0);
        const discoveredData = history.map((h) => h.discovered_count || 0);
        const committedData = history.map((h) => (h.committed ? 1 : 0));
        const latencyData = history.map((h) => h.elapsed_s || 0);
        const budgetData = history.map((h) => {
            const telemetry = h.telemetry || {};
            const pipeline = telemetry.pipeline || {};
            return typeof pipeline.budget === "number" ? pipeline.budget : null;
        });
        const wantsBudget = budgetData.some((value) => typeof value === "number");

        const actionsCanvas = document.getElementById("actionsChart");
        if (actionsCanvas) {
            new window.Chart(actionsCanvas.getContext("2d"), {
                type: "line",
                data: {
                    labels,
                    datasets: [
                        {
                            label: "Actions",
                            data: actionsData,
                            borderColor: "#58a6ff",
                            backgroundColor: "rgba(88,166,255,0.2)",
                            tension: 0.3,
                        },
                        {
                            label: "Discovered",
                            data: discoveredData,
                            borderColor: "#3fb950",
                            backgroundColor: "rgba(63,185,80,0.2)",
                            tension: 0.3,
                        },
                        {
                            label: "Committed",
                            data: committedData,
                            borderColor: "#ffdf5d",
                            backgroundColor: "rgba(255,223,93,0.2)",
                            tension: 0.1,
                            yAxisID: "y2",
                            stepped: true,
                        },
                    ],
                },
                options: {
                    plugins: {
                        legend: { labels: { color: "#e6edf3" } },
                    },
                    scales: {
                        x: {
                            ticks: { color: "#8b949e" },
                            grid: { color: "rgba(48,54,61,0.4)" },
                        },
                        y: {
                            ticks: { color: "#8b949e" },
                            grid: { color: "rgba(48,54,61,0.4)" },
                        },
                        y2: {
                            position: "right",
                            ticks: {
                                color: "#8b949e",
                                callback: (value) => (value ? "Yes" : "No"),
                            },
                            grid: { display: false },
                            suggestedMax: 1,
                            suggestedMin: 0,
                        },
                    },
                },
            });
        }

        const latencyCanvas = document.getElementById("latencyChart");
        if (latencyCanvas) {
            const datasets = [
                {
                    label: "Iteration Seconds",
                    data: latencyData,
                    borderColor: "#ff7b72",
                    backgroundColor: "rgba(255,123,114,0.25)",
                    tension: 0.2,
                },
            ];

            if (wantsBudget) {
                datasets.push({
                    label: "Budget",
                    data: budgetData,
                    borderColor: "#8b949e",
                    borderDash: [6, 6],
                    fill: false,
                });
            }

            new window.Chart(latencyCanvas.getContext("2d"), {
                type: "line",
                data: { labels, datasets },
                options: {
                    plugins: { legend: { labels: { color: "#e6edf3" } } },
                    scales: {
                        x: {
                            ticks: { color: "#8b949e" },
                            grid: { color: "rgba(48,54,61,0.4)" },
                        },
                        y: {
                            ticks: { color: "#8b949e" },
                            grid: { color: "rgba(48,54,61,0.4)" },
                        },
                    },
                },
            });
        }
    }

    const weightsCanvas = document.getElementById("weightsChart");
    if (hasChart && weightsCanvas && weightLabels.length) {
        const palette = [
            "#7afcff",
            "#f6a6ff",
            "#9effa9",
            "#ffe29a",
            "#b5b0ff",
            "#ffb8a5",
            "#aff8db",
            "#f3c4fb",
        ];
        const backgroundColors = weightLabels.map(
            (_, index) => palette[index % palette.length],
        );
        const totalWeight = weightValues.reduce((acc, value) => acc + value, 0);

        new window.Chart(weightsCanvas.getContext("2d"), {
            type: "doughnut",
            data: {
                labels: weightLabels,
                datasets: [
                    {
                        data: weightValues,
                        backgroundColor: backgroundColors,
                        borderColor: "#0d1117",
                        borderWidth: 2,
                    },
                ],
            },
            options: {
                cutout: "55%",
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label(context) {
                                const value = context.parsed || 0;
                                const percent = totalWeight
                                    ? ((value / totalWeight) * 100).toFixed(1)
                                    : "0.0";
                                return `${context.label}: ${value} (${percent}%)`;
                            },
                        },
                    },
                },
            },
        });

        const legendEl = document.getElementById("weightsLegend");
        if (legendEl) {
            legendEl.querySelectorAll("[data-index]").forEach((pill, index) => {
                const color = backgroundColors[index % backgroundColors.length];
                pill.style.borderColor = `${color}55`;
                const dot = pill.querySelector(".legend-dot");
                if (dot) {
                    dot.style.background = color;
                }
            });
        }

        const summary = weightsCanvas.getAttribute("data-summary");
        if (summary) {
            weightsCanvas.setAttribute("aria-label", summary);
        }
    }
})();
