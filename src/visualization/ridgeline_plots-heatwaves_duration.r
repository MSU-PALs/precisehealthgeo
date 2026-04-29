library(ggplot2)
library(ggridges)
library(dplyr)
library(tidyr)

root_dir <- "C:/Users/s1465450/OneDrive - University of Edinburgh/personal_training/precise-health-geo"
data_dir <- paste0(root_dir, "/data/processed/heat_waves/")

heatwave_data <- function(level) {
    out_df <- data.frame()
    
    for (source in c("ERA-5", "MERRA-2", "Dewpoint")) {
        file_path <- paste0(
            data_dir,
            level,
            "-level/",
            source,
            "_heat_waves.csv"
        )
        # Read extreme heat data
        data <- read.csv(file_path, stringsAsFactors = FALSE)
    
        # Create Exposure metric column
        data$`Exposure metric` <- ifelse(
            source == "Dewpoint",
            "ERA-5 T2MWET",
            paste0(source, " T2M")
        )    
        # Append
        out_df <- bind_rows(out_df, data)
    }    
    # Pivot to long format
    df_long <- out_df %>%
        rename_with(~ sub("_avg$", "", .x), ends_with("_avg")) %>%
        pivot_longer(
            cols = c("hw1a", "hw1b", "hw1c", "hw2a", "hw2b", "hw2c", "hw3a", "hw3b", "hw3c"),
            names_to = "heat_def",
            values_to = "proportion"
        ) %>%
        mutate(heat_def = toupper(heat_def))
  
    return(df_long)
}
# Define x-axis limits for each heat definition
xlims <- tibble::tibble(
    heat_def = c("HW1A", "HW1B", "HW1C", "HW2A", "HW2B", "HW2C", "HW3A", "HW3B", "HW3C"),
    xmin = c(0, 0, 0, 0, 0, 0, 0, 0, 0),
    xmax = c(40, 40, 40, 30, 30, 30, 20, 20, 20)
)
# Filter data based on x-axis limits for each heat definition
plot_data <- heatwave_data(level = "village") %>%
    left_join(xlims, by = "heat_def") %>%
    dplyr::filter(proportion >= xmin, proportion <= xmax)

final_plot <- ggplot(
    plot_data,
    aes(
        x = proportion,
        y = `Exposure metric`,
        fill = `Exposure metric`
    )
) +
geom_density_ridges(
    alpha = 0.6,
    scale = 1.1,
    # rel_min_height = 0.01,
    color = "grey30",
    size = 0.25
) +
facet_grid(
    Country ~ heat_def,
    scales = "free_x"
) +
scale_fill_brewer(palette = "Set2") +
labs(
    x = "Average duration of heatwave events (days)",
    y = "Density",
    fill = ""
) +
theme_classic(base_size = 12) +
theme_ridges() +
theme(
    legend.position = "bottom",
    strip.text = element_text(
        face = "bold",
        size = 11
    ),
    axis.text.y = element_blank(),
    axis.ticks.y = element_blank(),
    axis.title.y = element_text(hjust = 0.5),
    axis.title.x = element_text(
        hjust = 0.5,
        margin = margin(t = 8)
    ),
    panel.spacing.x = unit(0.8, "lines"),
    panel.spacing.y = unit(0.9, "lines"),
    plot.margin = margin(10, 15, 10, 10)
)
# Save the plot
ggsave(
    filename = paste0(root_dir, "/reports/ridgeline_heatwave-duration-village.png"),
    plot = final_plot,
    width = 12,
    height = 9,
    dpi = 600,
    bg = "white"
)
