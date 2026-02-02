
const outputMain = document.getElementById("mainOutput");

const formMeteo = document.getElementById("meteoForm");

formMeteo.addEventListener("submit", async (e) =>{
    e.preventDefault();

    const start = document.getElementById("meteo_start_date").value;
    const end = document.getElementById("meteo_end_date").value;
    const latitude = document.getElementById("latitude").value;
    const longitude = document.getElementById("longitude").value;

    outputMain.textContent = "Wysyłanie żądania ...";

    try {
        const response = await fetch(`/fetch_meteo?start_date=${start}&end_date=${end}&latitude=${latitude}&longitude=${longitude}`, {method: 'GET'});

        if (!response.ok) {
            const err = await response.json();
            outputMain.textContent = "Błąd " + err.detail;
            return;
        }

        const data = await response.text();
        outputMain.innerHTML = data;
    } catch (error) {
        outputMain.textContent = "Błąd połączenia: " + error;
    }

})

const formPse = document.getElementById("pseForm");
// const outputPse = document.getElementById("pseOutput");

formPse.addEventListener("submit", async (e) => {
    e.preventDefault();

    const start = document.getElementById("pse_start_date").value;
    const end = document.getElementById("pse_end_date").value;

    outputMain.textContent = "Wysyłanie żądania ...";

    try {
        const response = await fetch(`/fetch_generation?start_date=${start}&end_date=${end}`, {method: 'GET'});

        if (!response.ok) {
            const err = await response.json();
            outputMain.textContent = "Błąd " + err.detail;
            return;
        }

        const data = await response.text();
        outputMain.innerHTML = data;
    } catch (error) {
        outputMain.textContent = "Błąd połączenia: " + error;
    }

})

const formForecast = document.getElementById("forecastForm");
// const outputForecast = document.getElementById("forecastOutput");

formForecast.addEventListener("submit", async (e) =>{
    e.preventDefault();

    const latitude = document.getElementById("forecast_latitude").value;
    const longitude = document.getElementById("forecast_longitude").value;

    outputMain.textContent = "Wysyłanie żądania ...";

    try {
        const response = await fetch(`/fetch_meteo_forecast?latitude=${latitude}&longitude=${longitude}`, {method: 'GET'});

        if (!response.ok) {
            const err = await response.json();
            outputMain.textContent = "Błąd " + err.detail;
            return;
        }

        const data = await response.text();
        outputMain.innerHTML = data;
    } catch (error) {
        outputMain.textContent = "Błąd połączenia: " + error;
    }

})

const formUpload = document.getElementById("uploadForm");
// const outputUpload = document.getElementById("uploadOutput");

formUpload.addEventListener("submit", async (e) => {
        e.preventDefault();

        const fileInput = document.getElementById("fileInput");
        const formData = new FormData();
        formData.append("file", fileInput.files[0]);

        outputMain.textContent = "Wysyłanie pliku ...";

        try {
            const response = await fetch('/upload', {method: 'POST', body: formData});

            if (!response.ok) {
                const err = await response.json();
                outputMain.textContent = "Błąd " + err.detail;
                return;
            }

            const data = await response.text();
            outputMain.innerHTML = data;
        } catch (error) {
            outputMain.textContent = "Błąd połączenia: " + error;
        }
})

const formPredict = document.getElementById("predictForm");
// const outputPredict = document.getElementById("predictOutput");

formPredict.addEventListener("submit", async (e) =>{
    e.preventDefault();

    const model = document.getElementById("model_list").value;
    const steps = document.getElementById("steps").value;
    const n_lags = document.getElementById("n_lags").value;

    outputMain.textContent = "Wysyłanie żądania ...";

    try {

        if (!steps) {
            outputMain.textContent = "Podaj liczbę kroków!";
        return;
    }

    // window.open(`/create_forecast?steps=${steps}`, "_blank");

        const response = await fetch(`/create_forecast?model_name=${model}&steps=${steps}&n_lags=${n_lags}`, {method: 'GET'});

        if (!response.ok) {
            const err = await response.json();
            outputMain.textContent = "Błąd " + err.detail;
            return;
        }

        const html = await response.text();
        outputMain.innerHTML = html;
    } catch (error) {
        outputMain.textContent = "Błąd połączenia: " + error;
    }

})


const formGentab = document.getElementById("gentabForm");
// const outputGentab = document.getElementById("gentabOutput");

formGentab.addEventListener("submit", async (e) =>{
    e.preventDefault();

    outputMain.textContent = "Wysyłanie żądania ...";

    const table_name = document.getElementById("table_list").value;

    try {

        const response = await fetch(`/get_agg_data?table=${table_name}`, {method: 'GET'});

        if (!response.ok) {
            const err = await response.json();
            outputMain.textContent = "Błąd " + err.detail;
            return;
        }

        const html = await response.text();
        outputMain.innerHTML = html;
    } catch (error) {
        outputMain.textContent = "Błąd połączenia: " + error;
    }

})
;
