function getFirstDayOfWeek(d) {
    const date = new Date(d);
    const day = date.getDay();
    const diff = date.getDate() - day + (day === 0 ? -6 : 1);
    return new Date(date.setDate(diff));
}

(function () {
    let allTimeShippingSum = 0;
    let allTimeSalesSum = 0;
    let allTimeTotalSum = 0;
    let orderCount = 0;

    const weekTotals = {};
    const shippingTypeTotals = {};
    const shippingTypeCounts = {};
    const rows = document.querySelectorAll('table tr');

    rows.forEach(row => {
        if (!row.querySelector('td')) return;
        orderCount++;
        const dateStr = row.querySelector('td[data-label="Order Date"]')?.innerText;
        const shippingType = row.querySelector('td[data-label="Shipping Type"]')?.innerText;
        const shippingAmt = parseFloat(row.querySelector('td[data-label="Shipping Amt"]')?.innerText.replace('$', '')) || 0;
        const salesAmt = parseFloat(row.querySelector('td[data-label="Product Amt"]')?.innerText.replace('$', '')) || 0;
        const totalAmt = parseFloat(row.querySelector('td[data-label="Total Amt"]')?.innerText.replace('$', '')) || 0;

        allTimeShippingSum += shippingAmt;
        allTimeSalesSum += salesAmt;
        allTimeTotalSum += totalAmt;

        if (shippingType) {
            shippingTypeCounts[shippingType] = (shippingTypeCounts[shippingType] || 0) + 1;
            shippingTypeTotals[shippingType] = shippingTypeTotals[shippingType] || { shipping: 0, sales: 0, total: 0 };
            shippingTypeTotals[shippingType].shipping += shippingAmt;
            shippingTypeTotals[shippingType].sales += salesAmt;
            shippingTypeTotals[shippingType].total += totalAmt;
        }
        if (dateStr) {
            const date = new Date(dateStr.split(',')[0]);
            const firstDayOfWeek = getFirstDayOfWeek(date);
            const weekKey = `Week ${Math.ceil(date.getDate() / 7)} (${firstDayOfWeek.toISOString().split('T')[0]})`;

            weekTotals[weekKey] = weekTotals[weekKey] || { shipping: 0, sales: 0, total: 0, firstDayOfWeek };
            weekTotals[weekKey].shipping += shippingAmt;
            weekTotals[weekKey].sales += salesAmt;
            weekTotals[weekKey].total += totalAmt;
        }
    });
    const sortedWeekKeys = Object.keys(weekTotals).sort((a, b) => {
        return new Date(weekTotals[a].firstDayOfWeek) - new Date(weekTotals[b].firstDayOfWeek);
    });
    let csvContent = "data:text/csv;charset=utf-8,";
    csvContent += `All-Time Shipping Sum, All-Time Sales Sum, All-Time Total Sum, Total Orders\n`;
    csvContent += `${allTimeShippingSum.toFixed(2)}, ${allTimeSalesSum.toFixed(2)}, ${allTimeTotalSum.toFixed(2)}, ${orderCount}\n\n`;
    csvContent += `Week, Shipping, Sales, Total\n`;
    sortedWeekKeys.forEach(weekKey => {
        const totals = weekTotals[weekKey];
        csvContent += `${weekKey}, ${totals.shipping.toFixed(2)}, ${totals.sales.toFixed(2)}, ${totals.total.toFixed(2)}\n`;
    });
    csvContent += `\nShipping Type, Count, Shipping, Sales, Total\n`;
    Object.entries(shippingTypeTotals).forEach(([type, totals]) => {
        const count = shippingTypeCounts[type] || 0;
        csvContent += `${type}, ${count}, ${totals.shipping.toFixed(2)}, ${totals.sales.toFixed(2)}, ${totals.total.toFixed(2)}\n`;
    });
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement("a");
    link.setAttribute("href", encodedUri);
    link.setAttribute("download", "sales_report.csv");
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
})();