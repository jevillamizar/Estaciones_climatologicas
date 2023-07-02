document.addEventListener("DOMContentLoaded", function() {
    // Cargar el archivo JSON
    fetch("result.json")
        .then(response => response.json())
        .then(jsonData => {
            // Procesar los datos
            const countryData = jsonData.map(item => ({
                country: item.country,
                latitude: parseFloat(item.latitude),
                longitud: parseFloat(item.longitud)
            }));

            // Crear el gráfico
            const layout = {
                title: "Coordenadas de Estaciones Climatológicas",
                showlegend: false,
                height: 500,
                width: 800,
                margin: {
                    l: 50,
                    r: 50,
                    b: 50,
                    t: 80,
                    pad: 4
                }
            };

            const trace = {
                type: "scattergeo",
                lat: countryData.map(item => item.latitude),
                lon: countryData.map(item => item.longitud),
                mode: "markers",
                marker: {
                    size: 6,
                    color: "red",
                    opacity: 0.8
                }
            };

            const data = [trace];

            Plotly.newPlot("plot", data, layout);
        })
        .catch(error => console.error(error));
});