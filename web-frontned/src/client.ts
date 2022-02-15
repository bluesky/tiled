import axios from 'axios';

var axiosInstance = axios.create({
    baseURL: process.env.REACT_APP_API_PREFIX,
});

export interface IEntries {
    entries: string[];
}

export const search = async(segments: string[]): Promise<IEntries> => {
    const res = await axiosInstance.get('/node/search/' + segments.join('/'));
    console.log('search: ' + JSON.stringify(res.data));
    //var result = { hosts: res.data.hosts, success: true };
    let list: string[] = [];
    res.data.data.forEach((element: any) => {
        list.push(element.id)
    });
    console.log('items: ' + list);
    var result = { entries: list }
    return result;
}
