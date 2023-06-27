import * as functions from "firebase-functions";
import axios from "axios";
import * as admin from "firebase-admin";

admin.initializeApp();

const db = admin.database();

interface PremiseData {
    type: string;
    properties: {
        OBJECTID: string;
        [key: string]: any;
    };
    geometry: {
        type: string;
        coordinates: [number, number];
    };
    [key: string]: any;
}

exports.fetchPremisesData = functions.pubsub.schedule('0 0 1 * *')
    .timeZone('Europe/Brussels')
    .onRun(async (context) => {
        const result = await axios.get('https://geodata.antwerpen.be/arcgissql/rest/services/P_Portal/portal_publiek3/MapServer/201/query?where=1%3D1&outFields=*&outSR=4326&f=geojson');
        const apiData = result.data.features as PremiseData[];

        const promises = apiData.map(async (item) => {
            const existingData = await db.ref(`/Premises/${item.properties.OBJECTID}`).once('value');
            const existingDataVal = existingData.val();

            return db.ref(`/Premises/${item.properties.OBJECTID}`).update({
                type: item.type,
                properties: {
                    ...existingDataVal?.properties,
                    ...item.properties,
                    from_api: true
                },
                geometry: item.geometry,
                ...existingDataVal
            });
        });

        console.log(`Added ${promises.length} items`);

        return Promise.all(promises);
    });

