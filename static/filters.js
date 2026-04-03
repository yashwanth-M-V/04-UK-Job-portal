// ── Tab switching ──
const navTabs     = document.querySelectorAll(".nav-tab");
const panelJobs   = document.getElementById("panel-jobs");
const panelAnalytics = document.getElementById("panel-analytics");
const jobsFilters = document.getElementById("jobs-filters");

navTabs.forEach(tab => {
  tab.addEventListener("click", () => {
    navTabs.forEach(t => t.classList.remove("active"));
    tab.classList.add("active");

    if (tab.dataset.tab === "jobs") {
      panelJobs.style.display      = "block";
      panelAnalytics.style.display = "none";
      jobsFilters.style.display    = "block";
    } else {
      panelJobs.style.display      = "none";
      panelAnalytics.style.display = "block";
      jobsFilters.style.display    = "none";  // hide filters on analytics tab
    }
  });
});

